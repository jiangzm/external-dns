/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package alibabacloud

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"strings"

	"github.com/aliyun/alibaba-cloud-sdk-go/sdk"
	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/auth"
	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/auth/credentials"
	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/requests"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/alidns"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/pvtz"
	"github.com/goccy/go-yaml"
	log "github.com/sirupsen/logrus"

	"sigs.k8s.io/external-dns/endpoint"
	"sigs.k8s.io/external-dns/pkg/apis/externaldns"
	"sigs.k8s.io/external-dns/plan"
	"sigs.k8s.io/external-dns/provider"
)

const (
	apexRecord      = "@"
	defaultDnsTTL   = 600
	defaultPvtTTL   = 60
	defaultPageSize = 50
	defaultScheme   = "https"
	pvtzEndpoint    = "pvtz.aliyuncs.com"
	alidnsEndpoint  = "alidns.aliyuncs.com"
)

// AlibabaCloudDNSAPI is a minimal implementation of DNS API that we actually use, used primarily for unit testing.
// See https://help.aliyun.com/document_detail/29739.html for descriptions of all of its methods.
type AlibabaCloudDNSAPI interface {
	AddDomainRecord(request *alidns.AddDomainRecordRequest) (*alidns.AddDomainRecordResponse, error)
	DeleteDomainRecord(request *alidns.DeleteDomainRecordRequest) (*alidns.DeleteDomainRecordResponse, error)
	UpdateDomainRecord(request *alidns.UpdateDomainRecordRequest) (*alidns.UpdateDomainRecordResponse, error)
	DescribeDomainRecords(request *alidns.DescribeDomainRecordsRequest) (*alidns.DescribeDomainRecordsResponse, error)
	DescribeDomains(request *alidns.DescribeDomainsRequest) (*alidns.DescribeDomainsResponse, error)
}

// AlibabaCloudZoneAPI is a minimal implementation of Private Zone API that we actually use, used primarily for unit testing.
// See https://help.aliyun.com/document_detail/66234.html for descriptions of all of its methods.
type AlibabaCloudZoneAPI interface {
	AddZoneRecord(request *pvtz.AddZoneRecordRequest) (*pvtz.AddZoneRecordResponse, error)
	DeleteZoneRecord(request *pvtz.DeleteZoneRecordRequest) (*pvtz.DeleteZoneRecordResponse, error)
	UpdateZoneRecord(request *pvtz.UpdateZoneRecordRequest) (*pvtz.UpdateZoneRecordResponse, error)
	DescribeZoneRecords(request *pvtz.DescribeZoneRecordsRequest) (*pvtz.DescribeZoneRecordsResponse, error)
	DescribeZones(request *pvtz.DescribeZonesRequest) (*pvtz.DescribeZonesResponse, error)
	DescribeZoneInfo(request *pvtz.DescribeZoneInfoRequest) (*pvtz.DescribeZoneInfoResponse, error)
}

// AlibabaCloudProvider implements the DNS provider for Alibaba Cloud.
type AlibabaCloudProvider struct {
	provider.BaseProvider
	domainFilter *endpoint.DomainFilter
	zoneIDFilter provider.ZoneIDFilter // Private Zone only
	vpcID        string                // Private Zone only
	dryRun       bool
	dnsClient    AlibabaCloudDNSAPI
	pvtzClient   AlibabaCloudZoneAPI
	privateZone  bool
}

type alibabaCloudConfig struct {
	RegionID        string `json:"regionId"        yaml:"regionId"`
	AccessKeyID     string `json:"accessKeyId"     yaml:"accessKeyId"`
	AccessKeySecret string `json:"accessKeySecret" yaml:"accessKeySecret"`
	VPCID           string `json:"vpcId"           yaml:"vpcId"`
	RoleName        string `json:"roleName"        yaml:"roleName"` // For ECS RAM role only
	RoleArn         string `json:"roleArn"         yaml:"roleArn"`  // For OIDC RoleArn only
}

// New creates an Alibaba Cloud provider from the given configuration.
func New(_ context.Context, cfg *externaldns.Config, domainFilter *endpoint.DomainFilter) (provider.Provider, error) {
	return newProvider(cfg.AlibabaCloudConfigFile, domainFilter, provider.NewZoneIDFilter(cfg.ZoneIDFilter), cfg.AlibabaCloudZoneType, cfg.DryRun)
}

// newAlibabaCloudProvider creates a new Alibaba Cloud provider.
//
// Returns the provider or an error if a provider could not be created.
func newProvider(configFile string, domainFilter *endpoint.DomainFilter, zoneIDFileter provider.ZoneIDFilter, zoneType string, dryRun bool) (*AlibabaCloudProvider, error) {
	cfg := alibabaCloudConfig{}
	if configFile != "" {
		contents, err := os.ReadFile(configFile)
		if err != nil {
			return nil, fmt.Errorf("Failed to read Alibaba Cloud config file '%s': %w", configFile, err)
		}
		err = yaml.Unmarshal(contents, &cfg)
		if err != nil {
			return nil, fmt.Errorf("Failed to parse Alibaba Cloud config file '%s': %w", configFile, err)
		}
	}

	var err error
	var credential auth.Credential
	if cfg.AccessKeyID != "" {
		credential = &credentials.AccessKeyCredential{
			AccessKeyId:     cfg.AccessKeyID,
			AccessKeySecret: cfg.AccessKeySecret,
		}
	} else {
		roleName, roleArn := cfg.RoleName, cfg.RoleArn
		if roleName == "" && roleArn == "" {
			roleArn = os.Getenv("ALIBABA_CLOUD_ROLE_ARN")
			roleName, _ = getMetadata(MetadataKeys.RoleName)
		}
		if roleArn != "" {
			credential, err = credentials.NewOIDCCredentialsProviderBuilder().
				WithRoleArn(roleArn).
				WithRoleSessionName("external-dns").
				WithDurationSeconds(3600).
				Build()
		} else if roleName != "" {
			credential = &credentials.EcsRamRoleCredential{
				RoleName: roleName,
			}
		} else {
			credential = credentials.NewDefaultCredentialsProvider()
		}
	}

	// Public DNS service
	var dnsClient *alidns.Client
	// Private DNS service
	var pvtzClient *pvtz.Client

	privateZone := zoneType == "private"
	regionID, vpcID := cfg.RegionID, cfg.VPCID
	config := sdk.NewConfig().WithScheme(defaultScheme)

	if !privateZone {
		dnsClient, err := alidns.NewClientWithOptions(regionID, config, credential)
		if err != nil {
			return nil, fmt.Errorf("Failed to create Alibaba Cloud DNS client: %w", err)
		}
		dnsClient.Domain = alidnsEndpoint
	} else {
		if vpcID == "" {
			vpcID, _ = getMetadata(MetadataKeys.VpcID)
		}
		pvtzClient, err = pvtz.NewClientWithOptions(regionID, config, credential)
		if err != nil {
			return nil, fmt.Errorf("Failed to create Alibaba Cloud PrivateZone client: %w", err)
		}
		pvtzClient.Domain = pvtzEndpoint
	}

	provider := &AlibabaCloudProvider{
		domainFilter: domainFilter,
		zoneIDFilter: zoneIDFileter,
		vpcID:        vpcID,
		dryRun:       dryRun,
		dnsClient:    dnsClient,
		pvtzClient:   pvtzClient,
		privateZone:  privateZone,
	}

	return provider, nil
}

type MetadataKey string

var MetadataKeys = struct {
	VpcID    MetadataKey
	RegionID MetadataKey
	RoleName MetadataKey
}{
	VpcID:    "vpc-id",
	RegionID: "region-id",
	RoleName: "ram/security-credentials/",
}

// Get metadata bound to Alibaba Cloud ECS
// https://help.aliyun.com/en/ecs/user-guide/view-instance-metadata
// Return query results by metadata key
func getMetadata(key MetadataKey) (string, error) {
	baseURL := "http://100.100.100.200/latest/meta-data/"
	url := baseURL + strings.TrimPrefix(string(key), "/")
	res, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return "", fmt.Errorf("status code: %d", res.StatusCode)
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(string(body)), nil
}

// Records gets the current records.
//
// Returns the current records or an error if the operation failed.
func (p *AlibabaCloudProvider) Records(_ context.Context) ([]*endpoint.Endpoint, error) {
	if p.privateZone {
		return p.zoneRecords()
	}
	return p.dnsRecords()
}

// ApplyChanges applies the given changes.
//
// Returns nil if the operation was successful or an error if the operation failed.
func (p *AlibabaCloudProvider) ApplyChanges(_ context.Context, changes *plan.Changes) error {
	if changes == nil || !changes.HasChanges() {
		return nil
	}

	if p.privateZone {
		return p.applyChangesForZone(changes)
	}
	return p.applyChangesForDNS(changes)
}

// dnsRecords gets the current records.
//
// Returns the current records or an error if the operation failed.
func (p *AlibabaCloudProvider) dnsRecords() ([]*endpoint.Endpoint, error) {
	log.Infof("Retrieving Alibaba Cloud DNS Domain Records")
	domains, err := p.getDomains()
	if err != nil {
		return nil, err
	}
	endpoints := make([]*endpoint.Endpoint, 0)

	for _, domain := range domains {
		records, err := p.getDomainRecords(domain)
		if err != nil {
			return nil, err
		}

		endpointMap := make(map[string]*endpoint.Endpoint)
		for _, record := range records {
			if !provider.SupportedRecordType(record.Type) {
				continue
			}
			dnsname := getDNSName(record.RR, domain)
			key := toRecordKey(record.Type, dnsname, nil)

			if _, exist := endpointMap[key]; !exist {
				ttl := endpoint.TTL(record.TTL)
				endpointMap[key] = endpoint.NewEndpointWithTTL(dnsname, record.Type, ttl, record.Value)
			} else {
				endpointMap[key].Targets = append(endpointMap[key].Targets, record.Value)
			}
		}

		for _, ep := range endpointMap {
			endpoints = append(endpoints, ep)
		}
	}
	log.Infof("Found %d Alibaba Cloud DNS record(s).", len(endpoints))

	return endpoints, nil
}

func (p *AlibabaCloudProvider) getDomains() ([]string, error) {
	var domainNames []string
	request := alidns.CreateDescribeDomainsRequest()
	request.PageSize = requests.NewInteger(defaultPageSize)
	request.PageNumber = "1"
	for {
		resp, err := p.dnsClient.DescribeDomains(request)
		if err != nil {
			log.Errorf("Failed to describe domains for Alibaba Cloud DNS: %v", err)
			return nil, err
		}
		for _, item := range resp.Domains.Domain {
			if !p.domainFilter.IsConfigured() || p.domainFilter.Match(item.DomainName) {
				domainNames = append(domainNames, item.DomainName)
			}
		}

		if int(resp.TotalCount) <= len(domainNames) ||
			len(resp.Domains.Domain) < defaultPageSize {
			break
		}
		request.PageNumber = requests.NewInteger64(resp.PageNumber + 1)
	}
	return domainNames, nil
}

func (p *AlibabaCloudProvider) getDomainRecords(domain string) ([]*alidns.Record, error) {
	var records []*alidns.Record
	request := alidns.CreateDescribeDomainRecordsRequest()
	request.DomainName = domain
	request.PageSize = requests.NewInteger(defaultPageSize)
	request.PageNumber = "1"
	for {
		response, err := p.dnsClient.DescribeDomainRecords(request)
		if err != nil {
			log.Errorf("Failed to describe domain records for Alibaba Cloud DNS: %v", err)
			return nil, err
		}

		for _, record := range response.DomainRecords.Record {
			domainName := record.RR + "." + record.DomainName
			recordType := record.Type

			if !p.domainFilter.Match(domainName) || !provider.SupportedRecordType(recordType) {
				continue
			}
			// Use the same format as ExternalDNS
			record.Value = wrapWithQuotes(recordType, record.Value)
			records = append(records, &record)
		}

		if response.PageNumber*defaultPageSize >= response.TotalCount ||
			len(response.DomainRecords.Record) < defaultPageSize {
			break
		}
		request.PageNumber = requests.NewInteger64(response.PageNumber + 1)
	}

	return records, nil
}

func (p *AlibabaCloudProvider) applyChangesForDNS(changes *plan.Changes) error {
	log.Infof("ApplyChanges to Alibaba Cloud DNS: %++v", *changes)
	domains, err := p.getDomains()
	if err != nil {
		return fmt.Errorf("getting domain list: %w", err)
	}

	domainMapper := provider.ZoneIDName{}
	recordGroupMap := make(map[string][]*alidns.Record)
	for _, domain := range domains {
		domainMapper.Add(domain, domain)
		if records, err := p.getDomainRecords(domain); err == nil {
			for _, record := range records {
				key := toRecordKey(record.Type, domain, &record.RR)
				recordGroupMap[key] = append(recordGroupMap[key], record)
			}
		}
	}

	var errors []error
	addEndpoints := slices.Clone(changes.Create)
	delEndpoints := slices.Clone(changes.Delete)
	for i, ep := range changes.UpdateNew {
		key := toEndpointKey(ep)
		if records, exist := recordGroupMap[key]; exist {
			_, domain := domainMapper.FindZone(ep.DNSName)
			p.updateDomainRecords(domain, records, ep)
			continue
		}
		addEndpoints = append(addEndpoints, changes.UpdateNew[i])
	}

	for _, ep := range delEndpoints {
		key := toEndpointKey(ep)
		if records, exist := recordGroupMap[key]; exist {
			p.deleteDomainRecords(records, ep)
		}
	}

	for _, ep := range addEndpoints {
		_, domain := domainMapper.FindZone(ep.DNSName)
		p.createDomainRecords(domain, ep)
	}

	if len(errors) > 0 {
		log.Errorf("Failed to apply changes to Alibaba Cloud domain DNS: %v", errors)
	}

	return nil
}

func (p *AlibabaCloudProvider) createDomainRecords(domain string, ep *endpoint.Endpoint) error {
	if domain == "" || ep == nil {
		return fmt.Errorf("Invalid input for creating DNS record")
	}

	if p.dryRun {
		log.Infof("Dry run: create Alibaba Cloud domain DNS record %s with endpoint %v", domain, ep)
		return nil
	}

	var errs []error
	var subname = getSubName(ep.DNSName, domain)
	var ttl = resolveTTL(ep.RecordTTL, defaultDnsTTL)
	for _, target := range ep.Targets {
		req := alidns.CreateAddDomainRecordRequest()
		req.DomainName = domain
		req.Type = ep.RecordType
		req.RR = subname
		req.TTL = ttl
		req.Value = target
		if _, err := p.dnsClient.AddDomainRecord(req); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("Failed to create %d records: %v", len(errs), errs)
	}

	return nil
}

func (p *AlibabaCloudProvider) updateDomainRecords(domain string, records []*alidns.Record, ep *endpoint.Endpoint) error {
	if domain == "" || ep == nil {
		return fmt.Errorf("Invalid input for updating DNS record")
	}

	if p.dryRun {
		log.Infof("Dry run: update Alibaba Cloud domain DNS record %s with endpoint %v", domain, ep)
		return nil
	}

	var values []string
	for _, record := range records {
		values = append(values, record.Value)
	}
	var adds []string
	var dels []string
	if ep.RecordTTL.IsConfigured() && records[0].TTL != ep.GetRecordTTL() {
		adds, dels = ep.Targets, values
	} else {
		adds, dels, _ = provider.Difference(values, ep.Targets)
	}
	slices.Sort(adds)
	slices.Sort(dels)
	minlen := min(len(adds), len(dels))

	var errs []error
	var ttl = resolveTTL(ep.RecordTTL, defaultDnsTTL)
	if minlen > 0 {
		for i := range minlen {
			idx := slices.Index(values, dels[i])
			if idx >= 0 {
				req := alidns.CreateUpdateDomainRecordRequest()
				req.RecordId = records[idx].RecordId
				req.Value = adds[i]
				req.TTL = ttl
				if _, err := p.dnsClient.UpdateDomainRecord(req); err != nil {
					log.Errorf("Failed to update record '%s' in Alibaba Cloud DNS: %v", req.RecordId, err)
					errs = append(errs, err)
				}
			}
		}
	}
	if len(adds) > minlen {
		addEP := endpoint.NewEndpointWithTTL(ep.DNSName, ep.RecordType, ep.RecordTTL, adds[minlen:]...)
		if err := p.createDomainRecords(domain, addEP); err != nil {
			errs = append(errs, err)
		}
	} else if len(dels) > minlen {
		delEP := endpoint.NewEndpointWithTTL(ep.DNSName, ep.RecordType, ep.RecordTTL, dels[minlen:]...)
		if err := p.deleteDomainRecords(records, delEP); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("Failed to update %d records: %v", len(errs), errs)
	}

	return nil
}

func (p *AlibabaCloudProvider) deleteDomainRecords(records []*alidns.Record, ep *endpoint.Endpoint) error {
	if len(records) == 0 || ep == nil {
		return fmt.Errorf("Invalid input for deleting DNS record")
	}
	if p.dryRun {
		log.Infof("Dry run: delete Alibaba Cloud domain DNS records %v with endpoint %v", records, ep)
		return nil
	}
	var errs []error
	for _, target := range ep.Targets {
		if idx := slices.IndexFunc(records, func(r *alidns.Record) bool { return r.Value == target }); idx >= 0 {
			req := alidns.CreateDeleteDomainRecordRequest()
			req.RecordId = records[idx].RecordId
			if _, err := p.dnsClient.DeleteDomainRecord(req); err != nil {
				log.Errorf("Failed to delete record '%s' in Alibaba Cloud DNS: %v", req.RecordId, err)
				errs = append(errs, err)
			}
		} else {
			log.Errorf("Failed to find %s:%s record with value '%s' to delete", ep.RecordType, ep.DNSName, target)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("Failed to delete %d records: %v", len(errs), errs)
	}

	return nil
}

// recordsForPrivateZone gets the current records.
//
// Returns the current records or an error if the operation failed.
func (p *AlibabaCloudProvider) zoneRecords() ([]*endpoint.Endpoint, error) {
	log.Infof("Retrieving Alibaba Cloud zone DNS records")
	zones, err := p.getZones()
	if err != nil {
		return nil, err
	}

	endpoints := make([]*endpoint.Endpoint, 0)

	for _, zone := range zones {
		records, err := p.getZoneRecords(zone.ZoneId)
		if err != nil {
			return nil, err
		}

		endpointMap := make(map[string]*endpoint.Endpoint)
		for _, record := range records {
			if !provider.SupportedRecordType(record.Type) {
				continue
			}

			dnsname := getDNSName(record.Rr, zone.ZoneName)
			key := toRecordKey(record.Type, dnsname, nil)

			if _, exist := endpointMap[key]; !exist {
				ttl := endpoint.TTL(record.Ttl)
				endpointMap[key] = endpoint.NewEndpointWithTTL(dnsname, record.Type, ttl, record.Value)
			} else {
				endpointMap[key].Targets = append(endpointMap[key].Targets, record.Value)
			}
		}
		for _, ep := range endpointMap {
			endpoints = append(endpoints, ep)
		}
	}
	log.Infof("Found %d Alibaba Cloud zone DNS record(s).", len(endpoints))

	return endpoints, nil
}

func (p *AlibabaCloudProvider) getZones() ([]pvtz.Zone, error) {
	var zones []pvtz.Zone

	request := pvtz.CreateDescribeZonesRequest()
	request.PageSize = requests.NewInteger(defaultPageSize)
	request.PageNumber = "1"
	for {
		response, err := p.pvtzClient.DescribeZones(request)
		if err != nil {
			log.Errorf("Failed to describe zones in Alibaba Cloud DNS: %v", err)
			return nil, err
		}
		for _, zone := range response.Zones.Zone {
			if !p.zoneIDFilter.Match(zone.ZoneId) ||
				!p.domainFilter.Match(zone.ZoneName) ||
				!p.matchVPC(zone.ZoneId) {
				continue
			}
			zones = append(zones, zone)
		}
		if response.PageNumber*defaultPageSize >= response.TotalItems ||
			len(response.Zones.Zone) < defaultPageSize {
			break
		}
		request.PageNumber = requests.NewInteger(response.PageNumber + 1)
	}
	return zones, nil
}

func (p *AlibabaCloudProvider) getZoneRecords(zoneId string) ([]*pvtz.Record, error) {
	log.Infof("Retrieving Alibaba Cloud Private Zone records")
	var records []*pvtz.Record
	request := pvtz.CreateDescribeZoneRecordsRequest()
	request.ZoneId = zoneId
	request.PageSize = requests.NewInteger(defaultPageSize)
	request.PageNumber = "1"

	for {
		response, err := p.pvtzClient.DescribeZoneRecords(request)
		if err != nil {
			log.Errorf("Failed to describe zone record '%s' in Alibaba Cloud DNS: %v", zoneId, err)
			return nil, err
		}

		for _, record := range response.Records.Record {
			recordType := record.Type
			if !provider.SupportedRecordType(recordType) {
				continue
			}
			// Use the same format as ExternalDNS
			record.Value = wrapWithQuotes(recordType, record.Value)
			records = append(records, &record)
		}

		if response.PageNumber*defaultPageSize >= response.TotalItems ||
			len(response.Records.Record) < defaultPageSize {
			break
		}
		request.PageNumber = requests.NewInteger(response.PageNumber + 1)
	}

	return records, nil
}

// ApplyChanges applies the given changes.
//
// Returns nil if the operation was successful or an error if the operation failed.
func (p *AlibabaCloudProvider) applyChangesForZone(changes *plan.Changes) error {
	log.Infof("ApplyChanges to Alibaba Cloud Private Zone: %++v", *changes)
	zones, err := p.getZones()
	if err != nil {
		return fmt.Errorf("getting zone list: %w", err)
	}

	zoneMapper := provider.ZoneIDName{}
	recordGroupMap := make(map[string][]*pvtz.Record)
	for _, zone := range zones {
		zoneMapper.Add(zone.ZoneId, zone.ZoneName)
		if records, err := p.getZoneRecords(zone.ZoneId); err == nil {
			for _, record := range records {
				key := toRecordKey(record.Type, zone.ZoneName, &record.Rr)
				recordGroupMap[key] = append(recordGroupMap[key], record)
			}
		}
	}

	var errors []error
	addEndpoints := slices.Clone(changes.Create)
	delEndpoints := slices.Clone(changes.Delete)
	for i, ep := range changes.UpdateNew {
		key := toEndpointKey(ep)
		if records, exist := recordGroupMap[key]; exist {
			zoneId, zoneName := zoneMapper.FindZone(ep.DNSName)
			p.updateZoneRecords(zoneId, zoneName, records, ep)
			continue
		}
		addEndpoints = append(addEndpoints, changes.UpdateNew[i])
	}

	for _, ep := range delEndpoints {
		key := toEndpointKey(ep)
		if records, exist := recordGroupMap[key]; exist {
			p.deleteZoneRecords(records, ep)
		}
	}

	for _, ep := range addEndpoints {
		zoneId, zoneName := zoneMapper.FindZone(ep.DNSName)
		p.createZoneRecords(zoneId, zoneName, ep)
	}

	if len(errors) > 0 {
		log.Errorf("Failed to apply changes to Alibaba Cloud zone DNS: %v", errors)
	}

	return nil
}

func (p *AlibabaCloudProvider) createZoneRecords(zoneId, zoneName string, ep *endpoint.Endpoint) error {
	if zoneId == "" || zoneName == "" || ep == nil {
		return fmt.Errorf("Invalid input for creating zone record")
	}

	if p.dryRun {
		log.Infof("Dry run: create Alibaba Cloud domain zone record %s with endpoint %v", zoneName, ep)
		return nil
	}

	var errs []error
	var subname = getSubName(ep.DNSName, zoneName)
	var ttl = resolveTTL(ep.RecordTTL, defaultPvtTTL)
	for _, target := range ep.Targets {
		req := pvtz.CreateAddZoneRecordRequest()
		req.ZoneId = zoneId
		req.Type = ep.RecordType
		req.Rr = subname
		req.Ttl = ttl
		req.Value = target

		if _, err := p.pvtzClient.AddZoneRecord(req); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("Failed to create %d records: %v", len(errs), errs)
	}

	return nil
}

func (p *AlibabaCloudProvider) updateZoneRecords(zoneId, zoneName string, records []*pvtz.Record, ep *endpoint.Endpoint) error {
	if zoneId == "" || zoneName == "" || ep == nil {
		return fmt.Errorf("Invalid input for updating zone record")
	}

	if p.dryRun {
		log.Infof("Dry run: update Alibaba Cloud zone DNS records %s with endpoint %v", zoneName, ep)
		return nil
	}

	var values []string
	for _, record := range records {
		values = append(values, record.Value)
	}
	var adds []string
	var dels []string
	if ep.RecordTTL.IsConfigured() &&
		int64(records[0].Ttl) != ep.GetRecordTTL() {
		adds, dels = ep.Targets, values
	} else {
		adds, dels, _ = provider.Difference(values, ep.Targets)
	}
	slices.Sort(adds)
	slices.Sort(dels)
	minlen := min(len(adds), len(dels))

	var errs []error
	var ttl = resolveTTL(ep.RecordTTL, defaultDnsTTL)
	if minlen > 0 {
		for i := 0; i < minlen; i++ {
			idx := slices.Index(values, dels[i])
			if idx >= 0 {
				req := pvtz.CreateUpdateZoneRecordRequest()
				req.RecordId = requests.NewInteger64(records[idx].RecordId)
				req.Value = adds[i]
				req.Ttl = ttl
				if _, err := p.pvtzClient.UpdateZoneRecord(req); err != nil {
					log.Errorf("Failed to update record '%s' in Alibaba Cloud zone DNS: %v", req.RecordId, err)
					errs = append(errs, err)
				}
			}
		}
	}
	if len(adds) > minlen {
		addEP := endpoint.NewEndpointWithTTL(ep.DNSName, ep.RecordType, ep.RecordTTL, adds[minlen:]...)
		if err := p.createZoneRecords(zoneId, zoneName, addEP); err != nil {
			errs = append(errs, err)
		}
	} else if len(dels) > minlen {
		delEP := endpoint.NewEndpointWithTTL(ep.DNSName, ep.RecordType, ep.RecordTTL, dels[minlen:]...)
		if err := p.deleteZoneRecords(records, delEP); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("Failed to update %d records: %v", len(errs), errs)
	}

	return nil
}

func (p *AlibabaCloudProvider) deleteZoneRecords(records []*pvtz.Record, ep *endpoint.Endpoint) error {
	if len(records) == 0 || ep == nil {
		return fmt.Errorf("Invalid input for deleting zone DNS record")
	}
	if p.dryRun {
		log.Infof("Dry run: delete Alibaba Cloud zone DNS records %v with endpoint %v", records, ep)
		return nil
	}
	var errs []error
	for _, target := range ep.Targets {
		if idx := slices.IndexFunc(records, func(r *pvtz.Record) bool { return r.Value == target }); idx >= 0 {
			req := pvtz.CreateDeleteZoneRecordRequest()
			req.RecordId = requests.NewInteger64(records[idx].RecordId)
			if _, err := p.pvtzClient.DeleteZoneRecord(req); err != nil {
				log.Errorf("Failed to delete record '%s' in Alibaba Cloud DNS: %v", req.RecordId, err)
				errs = append(errs, err)
			}
		} else {
			log.Errorf("Failed to find %s:%s record with value '%s' to delete", ep.RecordType, ep.DNSName, target)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("Failed to delete %d records: %v", len(errs), errs)
	}

	return nil
}

func (p *AlibabaCloudProvider) matchVPC(zoneID string) bool {
	if p.vpcID == "" || zoneID == "" {
		return true
	}
	request := pvtz.CreateDescribeZoneInfoRequest()
	request.ZoneId = zoneID
	response, err := p.pvtzClient.DescribeZoneInfo(request)
	if err != nil {
		log.Errorf("Failed to describe zone info %s in Alibaba Cloud DNS: %v", zoneID, err)
		return false
	}
	for _, vpc := range response.BindVpcs.Vpc {
		if vpc.VpcId == p.vpcID {
			return true
		}
	}
	return false
}

func toRecordKey(recordType, domain string, subname *string) string {
	if subname != nil {
		domain = getDNSName(*subname, domain)
	}
	return fmt.Sprintf("%s:%s", recordType, domain)
}

func toEndpointKey(ep *endpoint.Endpoint) string {
	return fmt.Sprintf("%s:%s", ep.RecordType, ep.DNSName)
}

func resolveTTL(ttl endpoint.TTL, defaultTTL int) requests.Integer {
	if ttl.IsConfigured() {
		return requests.NewInteger64(int64(ttl))
	}
	return requests.NewInteger(int(defaultTTL))
}

func getSubName(dnsname, domain string) string {
	name := strings.TrimSuffix(dnsname, ".")
	name = strings.TrimSuffix(name, strings.TrimSuffix(domain, "."))
	name = strings.TrimSuffix(name, ".")

	if name == "" {
		return apexRecord
	}
	return name
}

func getDNSName(subname, domain string) string {
	subname = strings.Trim(subname, ".")
	domain = strings.Trim(domain, ".")
	if subname == apexRecord || subname == "" {
		return domain
	}
	if domain == "" {
		return subname
	}
	return subname + "." + domain
}

func unwrapQuotes(recordType, target string) string {
	if recordType == endpoint.RecordTypeTXT && strings.HasPrefix(target, `"heritage=`) {
		return strings.Trim(target, `"`)
	}
	return target
}

func wrapWithQuotes(recordType, value string) string {
	if recordType == endpoint.RecordTypeTXT && strings.HasPrefix(value, `heritage=`) {
		// Alibaba Cloud returns TXT record values without quotes.
		// Restore the quotes to match ExternalDNS's expected format.
		return fmt.Sprintf("%q", value)
	}
	return value
}
