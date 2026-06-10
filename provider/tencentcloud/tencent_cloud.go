/*
Copyright 2022 The Kubernetes Authors.

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

package tencentcloud

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/errors"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/profile"
	dnspod "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/dnspod/v20210323"
	privatedns "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/privatedns/v20201028"
	"sigs.k8s.io/external-dns/endpoint"
	"sigs.k8s.io/external-dns/pkg/apis/externaldns"
	"sigs.k8s.io/external-dns/plan"
	"sigs.k8s.io/external-dns/provider"
)

const (
	apexRecord    = "@"
	defaultDnsTTL = 600
	defaultPvtTTL = 60
	pageSize      = 100
	dnsDomain     = "dnspod.tencentcloudapi.com"     // Public  DNS for Internet
	zoneDomain    = "privatedns.tencentcloudapi.com" // Private DNS for VPC
)

type PublicDNSAPI interface {
	CreateRecord(req *dnspod.CreateRecordRequest) (res *dnspod.CreateRecordResponse, err error)
	DeleteRecord(req *dnspod.DeleteRecordRequest) (res *dnspod.DeleteRecordResponse, err error)
	ModifyRecord(req *dnspod.ModifyRecordRequest) (res *dnspod.ModifyRecordResponse, err error)
	DescribeDomainList(req *dnspod.DescribeDomainListRequest) (res *dnspod.DescribeDomainListResponse, err error)
	DescribeRecordList(req *dnspod.DescribeRecordListRequest) (res *dnspod.DescribeRecordListResponse, err error)
}

type PrivateDNSAPI interface {
	CreatePrivateZoneRecord(req *privatedns.CreatePrivateZoneRecordRequest) (res *privatedns.CreatePrivateZoneRecordResponse, err error)
	DeletePrivateZoneRecord(req *privatedns.DeletePrivateZoneRecordRequest) (res *privatedns.DeletePrivateZoneRecordResponse, err error)
	ModifyPrivateZoneRecord(req *privatedns.ModifyPrivateZoneRecordRequest) (res *privatedns.ModifyPrivateZoneRecordResponse, err error)
	DescribePrivateZoneList(req *privatedns.DescribePrivateZoneListRequest) (res *privatedns.DescribePrivateZoneListResponse, err error)
	DescribePrivateZoneRecordList(req *privatedns.DescribePrivateZoneRecordListRequest) (res *privatedns.DescribePrivateZoneRecordListResponse, err error)
}

type TencentCloudProvider struct {
	provider.BaseProvider
	domainFilter *endpoint.DomainFilter
	zoneIDFilter *provider.ZoneIDFilter // Private Zone only
	vpcID        string                 // Private Zone only
	dryRun       bool
	privateZone  bool
	dnsApi       PublicDNSAPI
	pvtApi       PrivateDNSAPI
}

type tencentCloudConfig struct {
	RegionId  string `json:"regionId"    yaml:"regionId"`
	SecretId  string `json:"secretId"    yaml:"secretId"`
	SecretKey string `json:"secretKey"   yaml:"secretKey"`
	VPCId     string `json:"vpcId"       yaml:"vpcId"`
	RoleName  string `json:"roleName"    yaml:"roleName"` // For CVM RAM role only
	RoleArn   string `json:"roleArn"     yaml:"roleArn"`  // For OIDC RoleArn only
}

// New creates an Tencent Cloud provider from the given configuration.
func New(_ context.Context, cfg *externaldns.Config, domainFilter *endpoint.DomainFilter) (provider.Provider, error) {
	return newProvider(cfg.TencentCloudConfigFile, domainFilter, provider.NewZoneIDFilter(cfg.ZoneIDFilter), cfg.TencentCloudZoneType, cfg.DryRun)
}

// newProvider creates a new Tencent Cloud provider.
//
// Returns the provider or an error if a provider could not be created.
func newProvider(configFile string, domainFilter *endpoint.DomainFilter, zoneIDFilter provider.ZoneIDFilter, zoneType string, dryRun bool) (*TencentCloudProvider, error) {
	cfg := tencentCloudConfig{}
	if configFile != "" {
		contents, err := os.ReadFile(configFile)
		if err != nil {
			return nil, fmt.Errorf("Failed to read TencentCloud config file '%s': %w", configFile, err)
		}
		err = json.Unmarshal(contents, &cfg)
		if err != nil {
			return nil, fmt.Errorf("Failed to parse TencentCloud config file '%s': %w", configFile, err)
		}
	}

	var err error
	var credential common.CredentialIface
	if cfg.SecretId != "" {
		credential = common.NewCredential(cfg.SecretId, cfg.SecretKey)
	} else {
		roleName, roleArn := cfg.RoleName, cfg.RoleArn
		if roleName == "" && roleArn == "" {
			roleArn = os.Getenv("TKE_ROLE_ARN")
			roleName = ""
		}
		if roleArn != "" {
			provider, _ := common.DefaultTkeOIDCRoleArnProvider()
			credential, err = provider.GetCredential()
		} else if roleName != "" {
			credential, err = common.NewCvmRoleProvider(roleName).GetCredential()
		}
	}

	if err != nil {
		return nil, fmt.Errorf("Failed to create TencentCloud Credential: %w", err)
	}

	// Public DNS service
	dnsProfile := profile.NewClientProfile()
	dnsProfile.HttpProfile.Endpoint = dnsDomain
	dnsApi, err := dnspod.NewClient(credential, cfg.RegionId, dnsProfile)

	if err != nil {
		return nil, fmt.Errorf("Failed to create TencentCloud DNSPod client: %w", err)
	}

	// Private DNS service
	zoneProfile := profile.NewClientProfile()
	zoneProfile.HttpProfile.Endpoint = zoneDomain
	zoneApi, err := privatedns.NewClient(credential, cfg.RegionId, zoneProfile)

	if err != nil {
		return nil, fmt.Errorf("Failed to create TencentCloud PrivateDNS client: %w", err)
	}

	provider := &TencentCloudProvider{
		domainFilter: domainFilter,
		zoneIDFilter: &zoneIDFilter,
		dryRun:       dryRun,
		dnsApi:       dnsApi,
		pvtApi:       zoneApi,
		vpcID:        cfg.VPCId,
		privateZone:  zoneType == "private",
	}

	return provider, nil
}

func (p *TencentCloudProvider) Records(ctx context.Context) ([]*endpoint.Endpoint, error) {
	if p.privateZone {
		return p.zoneRecords()
	}
	return p.dnsRecords()
}

func (p *TencentCloudProvider) ApplyChanges(ctx context.Context, changes *plan.Changes) error {
	if !changes.HasChanges() {
		return nil
	}

	if p.privateZone {
		return p.applyChangesForZone(changes)
	}

	return p.applyChangesForDNS(changes)
}

func (p *TencentCloudProvider) dnsRecords() ([]*endpoint.Endpoint, error) {
	log.Infof("Retrieving Tencent Cloud domain DNS records")
	domains, err := p.getDomains()
	if err != nil {
		return nil, err
	}
	endpoints := make([]*endpoint.Endpoint, 0)

	for _, domain := range domains {
		domainName := *domain.Name
		records, err := p.getDomainRecords(domainName, *domain.DomainId)
		if err != nil {
			return nil, err
		}

		endpointMap := make(map[string]*endpoint.Endpoint)
		for _, record := range records {
			if !provider.SupportedRecordType(*record.Type) {
				continue
			}

			name := getDNSName(*record.Name, domainName)
			key := toRecordKey(*record.Type, name, "")

			if _, exist := endpointMap[key]; !exist {
				ttl := endpoint.TTL(*record.TTL)
				endpointMap[key] = endpoint.NewEndpointWithTTL(name, *record.Type, ttl, *record.Value)
			} else {
				endpointMap[key].Targets = append(endpointMap[key].Targets, *record.Value)
			}
		}

		for _, ep := range endpointMap {
			endpoints = append(endpoints, ep)
		}
	}
	log.Infof("Found %d TencentCloud domain DNS record(s).", len(endpoints))

	return endpoints, nil
}

func (p *TencentCloudProvider) getDomains() ([]*dnspod.DomainListItem, error) {
	request := dnspod.NewDescribeDomainListRequest()
	request.Offset = common.Int64Ptr(0)
	request.Limit = common.Int64Ptr(pageSize)

	domainList := make([]*dnspod.DomainListItem, 0)
	totalCount := int64(pageSize)
	for *request.Offset < totalCount {
		response, err := p.dnsApi.DescribeDomainList(request)
		if err != nil {
			return nil, err
		}

		for _, domain := range response.Response.DomainList {
			if p.domainFilter == nil || !p.domainFilter.IsConfigured() || p.domainFilter.Match(*domain.Name) {
				domainList = append(domainList, domain)
			}
		}

		if response.Response.DomainCountInfo == nil ||
			response.Response.DomainCountInfo.AllTotal == nil ||
			len(response.Response.DomainList) < pageSize {
			break
		}
		totalCount = int64(*response.Response.DomainCountInfo.AllTotal)
		*request.Offset += int64(len(response.Response.DomainList))
	}
	return domainList, nil
}

func (p *TencentCloudProvider) getDomainRecords(domain string, domainId uint64) ([]*dnspod.RecordListItem, error) {
	request := dnspod.NewDescribeRecordListRequest()
	request.Domain = &domain
	request.DomainId = &domainId
	request.Offset = common.Uint64Ptr(0)
	request.Limit = common.Uint64Ptr(pageSize)

	recordList := make([]*dnspod.RecordListItem, 0)
	totalCount := uint64(pageSize)
	for *request.Offset < totalCount {
		response, err := p.dnsApi.DescribeRecordList(request)
		if err != nil {
			// No data is not an error
			if apiErr, ok := err.(*errors.TencentCloudSDKError); ok && apiErr.Code == dnspod.RESOURCENOTFOUND_NODATAOFRECORD {
				reqstr, _ := json.Marshal(*request)
				log.Infof("DescribeRecordList request: %s, error: %+v", string(reqstr), *apiErr)
				return recordList, nil
			}
			return nil, err
		}

		for _, record := range response.Response.RecordList {
			if *record.Name == apexRecord && *record.Type == endpoint.RecordTypeNS {
				continue
			}
			*record.Value = wrapWithQuotes(*record.Type, *record.Value)
			recordList = append(recordList, record)
		}

		if response.Response.RecordCountInfo == nil ||
			response.Response.RecordCountInfo.TotalCount == nil ||
			int(*response.Response.RecordCountInfo.TotalCount) == len(recordList) ||
			len(response.Response.RecordList) < pageSize {
			break
		}
		totalCount = *response.Response.RecordCountInfo.TotalCount
		*request.Offset += uint64(len(response.Response.RecordList))
	}
	return recordList, nil
}

func (p *TencentCloudProvider) applyChangesForDNS(changes *plan.Changes) error {
	log.Infof("Apply changes to TencentCloud domain DNS: %+v", *changes)
	domains, err := p.getDomains()
	if err != nil {
		return err
	}

	zoneIDMapper := provider.ZoneIDName{}
	recordGroupMap := make(map[string][]*dnspod.RecordListItem)
	for _, domain := range domains {
		zoneIDMapper.Add(strconv.FormatUint(*domain.DomainId, 10), *domain.Name)
		if records, err := p.getDomainRecords(*domain.Name, *domain.DomainId); err == nil {
			for _, record := range records {
				key := toRecordKey(*record.Type, *domain.Name, *record.Name)
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
			zoneId, domain := zoneIDMapper.FindZone(ep.DNSName)
			domainId, _ := strconv.ParseUint(zoneId, 10, 64)
			if err := p.updateDomainRecords(domainId, domain, records, ep); err != nil {
				errors = append(errors, err)
			}
			continue
		}
		addEndpoints = append(addEndpoints, changes.UpdateNew[i])
	}

	for _, ep := range delEndpoints {
		key := toEndpointKey(ep)
		if records, exist := recordGroupMap[key]; exist {
			zoneId, domain := zoneIDMapper.FindZone(ep.DNSName)
			domainId, _ := strconv.ParseUint(zoneId, 10, 64)
			if err := p.deleteDomainRecords(domainId, domain, records, ep); err != nil {
				errors = append(errors, err)
			}
		}
	}

	for _, ep := range addEndpoints {
		zoneId, domain := zoneIDMapper.FindZone(ep.DNSName)
		domainId, _ := strconv.ParseUint(zoneId, 10, 64)
		if err := p.createDomainRecords(domainId, domain, ep); err != nil {
			errors = append(errors, err)
		}
	}

	if len(errors) > 0 {
		log.Errorf("Failed to apply changes to TencentCloud domain DNS: %v", errors)
	}

	return nil
}

func (p *TencentCloudProvider) createDomainRecords(domainId uint64, domain string, ep *endpoint.Endpoint) error {
	if domainId == 0 || domain == "" || ep == nil {
		return fmt.Errorf("Invalid input for creating DNS record")
	}

	if p.dryRun {
		log.Infof("Dry run: create Tencent Cloud domain DNS record %s with endpoint %v", domain, ep)
		return nil
	}

	var errs []error
	var subDomain = getSubDomain(ep.DNSName, domain)
	var ttl = resolveTTL[uint64](ep.RecordTTL, defaultDnsTTL)
	for _, target := range ep.Targets {
		target := unwrapQuotes(ep.RecordType, target)
		req := dnspod.NewCreateRecordRequest()
		req.Domain = &domain
		req.DomainId = &domainId
		req.SubDomain = &subDomain
		req.RecordType = &ep.RecordType
		req.RecordLine = common.StringPtr("默认")
		req.TTL = &ttl
		req.Value = &target
		if _, err := p.dnsApi.CreateRecord(req); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("Failed to create %d records: %v", len(errs), errs)
	}

	return nil
}

func (p *TencentCloudProvider) updateDomainRecords(domainId uint64, domain string, records []*dnspod.RecordListItem, ep *endpoint.Endpoint) error {
	if domainId == 0 || domain == "" || len(records) == 0 || ep == nil {
		return fmt.Errorf("Invalid input for updating DNS record")
	}

	if p.dryRun {
		log.Infof("Dry run: update TencentCloud domain DNS record %s with endpoint %v", domain, ep)
		return nil
	}

	var values []string
	for _, record := range records {
		values = append(values, *record.Value)
	}
	var adds []string
	var dels []string
	if ep.RecordTTL.IsConfigured() && *records[0].TTL != uint64(ep.RecordTTL) {
		adds, dels = ep.Targets, values
	} else {
		adds, dels, _ = provider.Difference(values, ep.Targets)
	}
	slices.Sort(adds)
	slices.Sort(dels)
	minlen := min(len(adds), len(dels))

	var errs []error
	var ttl = resolveTTL[uint64](ep.RecordTTL, defaultDnsTTL)
	if minlen > 0 {
		request := dnspod.NewModifyRecordRequest()
		request.Domain = &domain
		request.DomainId = &domainId
		request.RecordLine = common.StringPtr("默认")
		request.TTL = &ttl

		for i := 0; i < minlen; i++ {
			idx := slices.Index(values, dels[i])
			if idx >= 0 {
				req := *request
				req.Value = &adds[i]
				req.RecordId = records[idx].RecordId
				if _, err := p.dnsApi.ModifyRecord(&req); err != nil {
					log.Errorf("Failed to update record '%d' in TencentCloud DNS: %v", *req.RecordId, err)
					errs = append(errs, err)
				}
			}
		}
	}
	if len(adds) > minlen {
		addEP := endpoint.NewEndpointWithTTL(ep.DNSName, ep.RecordType, ep.RecordTTL, adds[minlen:]...)
		if err := p.createDomainRecords(domainId, domain, addEP); err != nil {
			errs = append(errs, err)
		}
	} else if len(dels) > minlen {
		delEP := endpoint.NewEndpointWithTTL(ep.DNSName, ep.RecordType, ep.RecordTTL, dels[minlen:]...)
		if err := p.deleteDomainRecords(domainId, domain, records, delEP); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("Failed to update %d records: %v", len(errs), errs)
	}

	return nil
}

func (p *TencentCloudProvider) deleteDomainRecords(domainId uint64, domain string, records []*dnspod.RecordListItem, ep *endpoint.Endpoint) error {
	if domainId == 0 || domain == "" || len(records) == 0 || ep == nil {
		return fmt.Errorf("Invalid input for deleting DNS record")
	}
	if p.dryRun {
		log.Infof("Dry run: delete TencentCloud domain DNS records %v with endpoint %v", records, ep)
		return nil
	}
	var errs []error
	for _, target := range ep.Targets {
		if idx := slices.IndexFunc(records, func(r *dnspod.RecordListItem) bool { return *r.Value == target }); idx >= 0 {
			req := dnspod.NewDeleteRecordRequest()
			req.Domain = &domain
			req.DomainId = &domainId
			req.RecordId = records[idx].RecordId
			if _, err := p.dnsApi.DeleteRecord(req); err != nil {
				log.Errorf("Failed to delete record '%d' in TencentCloud DNS: %v", req.RecordId, err)
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

func (p *TencentCloudProvider) zoneRecords() ([]*endpoint.Endpoint, error) {
	log.Infof("Retrieving TencentCloud zone DNS records")
	zones, err := p.getZones()
	if err != nil {
		return nil, err
	}

	endpoints := make([]*endpoint.Endpoint, 0)

	for _, zone := range zones {
		records, err := p.getZoneRecords(*zone.ZoneId)
		if err != nil {
			return nil, err
		}

		endpointMap := make(map[string]*endpoint.Endpoint)
		for _, record := range records {
			if !provider.SupportedRecordType(*record.RecordType) {
				continue
			}

			name := getDNSName(*record.SubDomain, *zone.Domain)
			key := toRecordKey(*record.RecordType, name, "")

			if _, exist := endpointMap[key]; !exist {
				ttl := endpoint.TTL(*record.TTL)
				endpointMap[key] = endpoint.NewEndpointWithTTL(name, *record.RecordType, ttl, *record.RecordValue)
			} else {
				endpointMap[key].Targets = append(endpointMap[key].Targets, *record.RecordValue)
			}
		}
		for _, ep := range endpointMap {
			endpoints = append(endpoints, ep)
		}
	}
	log.Infof("Found %d TencentCloud zone DNS record(s).", len(endpoints))

	return endpoints, nil
}

func (p *TencentCloudProvider) getZones() ([]*privatedns.PrivateZone, error) {
	filters := []*privatedns.Filter{
		{
			Name: common.StringPtr("Vpc"),
			Values: []*string{
				common.StringPtr(p.vpcID),
			},
		},
	}

	if p.zoneIDFilter != nil && p.zoneIDFilter.IsConfigured() {
		zoneIds := make([]*string, len(p.zoneIDFilter.ZoneIDs))
		for index, zoneId := range p.zoneIDFilter.ZoneIDs {
			zoneIds[index] = common.StringPtr(zoneId)
		}
		filters = append(filters, &privatedns.Filter{
			Name:   common.StringPtr("ZoneId"),
			Values: zoneIds,
		})
	}

	request := privatedns.NewDescribePrivateZoneListRequest()
	request.Filters = filters
	request.Offset = common.Int64Ptr(0)
	request.Limit = common.Int64Ptr(pageSize)

	privateZones := make([]*privatedns.PrivateZone, 0)
	totalCount := int64(pageSize)
	for *request.Offset < totalCount {
		response, err := p.pvtApi.DescribePrivateZoneList(request)
		if err != nil {
			return nil, err
		}

		for _, privateZone := range response.Response.PrivateZoneSet {
			if p.domainFilter != nil && !p.domainFilter.Match(*privateZone.Domain) {
				continue
			}
			privateZones = append(privateZones, privateZone)
		}

		if response.Response.TotalCount == nil || len(response.Response.PrivateZoneSet) < pageSize {
			break
		}
		totalCount = *response.Response.TotalCount
		*request.Offset += int64(len(response.Response.PrivateZoneSet))
	}

	return privateZones, nil
}

func (p *TencentCloudProvider) getZoneRecords(zoneID string) ([]*privatedns.PrivateZoneRecord, error) {
	request := privatedns.NewDescribePrivateZoneRecordListRequest()
	request.ZoneId = common.StringPtr(zoneID)
	request.Offset = common.Int64Ptr(0)
	request.Limit = common.Int64Ptr(pageSize)

	records := make([]*privatedns.PrivateZoneRecord, 0)
	totalCount := int64(pageSize)
	for *request.Offset < totalCount {
		response, err := p.pvtApi.DescribePrivateZoneRecordList(request)
		if err != nil {
			return nil, err
		}

		for _, record := range response.Response.RecordSet {
			if *record.SubDomain == apexRecord && (*record.RecordType == endpoint.RecordTypeNS ||
				(*record.RecordType == endpoint.RecordTypeTXT && *record.RecordValue == "tencent_provider_record")) {
				continue
			}
			*record.RecordValue = wrapWithQuotes(*record.RecordType, *record.RecordValue)
			records = append(records, record)
		}

		if response.Response.TotalCount == nil ||
			int(*response.Response.TotalCount) == len(records) ||
			len(response.Response.RecordSet) < pageSize {
			break
		}
		totalCount = *response.Response.TotalCount
		*request.Offset += int64(len(response.Response.RecordSet))
	}
	return records, nil
}

func (p *TencentCloudProvider) applyChangesForZone(changes *plan.Changes) error {
	log.Infof("Apply changes to TencentCloud zone DNS: %+v", *changes)
	zones, err := p.getZones()
	if err != nil {
		return err
	}

	zoneMapper := provider.ZoneIDName{}
	recordGroupMap := make(map[string][]*privatedns.PrivateZoneRecord)
	for _, zone := range zones {
		zoneMapper.Add(*zone.ZoneId, *zone.Domain)
		if records, err := p.getZoneRecords(*zone.ZoneId); err == nil {
			for _, record := range records {
				key := toRecordKey(*record.RecordType, *zone.Domain, *record.SubDomain)
				recordGroupMap[key] = append(recordGroupMap[key], record)
			}
		}

		// Tencent Cloud PrivateDNS requires each private zone to keep at least one record.
		baseEP := endpoint.NewEndpoint(*zone.Domain, endpoint.RecordTypeTXT, "tencent_provider_record")
		baseKey := toRecordKey(baseEP.RecordType, *zone.Domain, "")
		if records, exist := recordGroupMap[baseKey]; !exist || len(records) == 0 {
			if err := p.createZoneRecords(*zone.ZoneId, *zone.Domain, baseEP); err != nil {
				return err
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
			if err := p.updateZoneRecords(zoneId, zoneName, records, ep); err != nil {
				errors = append(errors, err)
			}
			continue
		}
		addEndpoints = append(addEndpoints, changes.UpdateNew[i])
	}

	for _, ep := range delEndpoints {
		key := toEndpointKey(ep)
		zoneId, _ := zoneMapper.FindZone(ep.DNSName)
		if records, exist := recordGroupMap[key]; exist {
			if err := p.deleteZoneRecords(zoneId, records, ep); err != nil {
				errors = append(errors, err)
			}
		}
	}

	for _, ep := range addEndpoints {
		zoneId, zoneName := zoneMapper.FindZone(ep.DNSName)
		if err := p.createZoneRecords(zoneId, zoneName, ep); err != nil {
			errors = append(errors, err)
		}
	}

	if len(errors) > 0 {
		log.Errorf("Failed to apply changes to TencentCloud zone DNS: %v", errors)
	}

	return nil
}

func (p *TencentCloudProvider) createZoneRecords(zoneId string, zoneName string, ep *endpoint.Endpoint) error {
	if zoneId == "" || zoneName == "" || ep == nil {
		return fmt.Errorf("Invalid input for creating zone DNS record")
	}

	if p.dryRun {
		log.Infof("Dry run: create TencentCloud zone DNS record %s with endpoint %v", zoneId, ep)
		return nil
	}

	subDomain := getSubDomain(ep.DNSName, zoneName)
	ttl := resolveTTL[int64](ep.RecordTTL, defaultPvtTTL)

	request := privatedns.NewCreatePrivateZoneRecordRequest()
	request.ZoneId = &zoneId
	request.SubDomain = &subDomain
	request.RecordType = &ep.RecordType
	request.TTL = &ttl

	var errs []error
	for _, target := range ep.Targets {
		target := unwrapQuotes(ep.RecordType, target)
		req := *request
		req.RecordValue = &target

		if _, err := p.pvtApi.CreatePrivateZoneRecord(&req); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("Failed to create %d records: %v", len(errs), errs)
	}

	return nil
}

func (p *TencentCloudProvider) updateZoneRecords(zoneId string, zoneName string, records []*privatedns.PrivateZoneRecord, ep *endpoint.Endpoint) error {
	if zoneId == "" || zoneName == "" || len(records) == 0 || ep == nil {
		return fmt.Errorf("Invalid input for updating zone DNS record")
	}

	if p.dryRun {
		log.Infof("Dry run: update TencentCloud zone DNS record %s with endpoint %v", zoneId, ep)
		return nil
	}

	var values []string
	for _, record := range records {
		values = append(values, *record.RecordValue)
	}
	var adds []string
	var dels []string
	if ep.RecordTTL.IsConfigured() && *records[0].TTL != int64(ep.RecordTTL) {
		adds, dels = ep.Targets, values
	} else {
		adds, dels, _ = provider.Difference(values, ep.Targets)
	}
	slices.Sort(adds)
	slices.Sort(dels)
	minlen := min(len(adds), len(dels))
	ttl := resolveTTL[int64](ep.RecordTTL, defaultPvtTTL)

	var errs []error
	if minlen > 0 {
		request := privatedns.NewModifyPrivateZoneRecordRequest()
		request.ZoneId = &zoneId
		request.TTL = &ttl

		for i := 0; i < minlen; i++ {
			idx := slices.Index(values, dels[i])
			if idx >= 0 {
				req := *request
				req.RecordValue = &adds[i]
				req.RecordId = records[idx].RecordId
				if _, err := p.pvtApi.ModifyPrivateZoneRecord(&req); err != nil {
					log.Errorf("Failed to update record '%s' in TencentCloud zone DNS: %v", *req.RecordId, err)
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
		if err := p.deleteZoneRecords(zoneId, records, delEP); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("Failed to update %d records: %v", len(errs), errs)
	}

	return nil
}

func (p *TencentCloudProvider) deleteZoneRecords(zoneId string, records []*privatedns.PrivateZoneRecord, ep *endpoint.Endpoint) error {
	if zoneId == "" || len(records) == 0 || ep == nil {
		return fmt.Errorf("Invalid input for deleting DNS record")
	}
	if p.dryRun {
		log.Infof("Dry run: delete TencentCloud domain DNS records %v with endpoint %v", records, ep)
		return nil
	}

	var errs []error
	for _, target := range ep.Targets {
		if idx := slices.IndexFunc(records, func(r *privatedns.PrivateZoneRecord) bool { return *r.RecordValue == target }); idx >= 0 {
			req := privatedns.NewDeletePrivateZoneRecordRequest()
			req.ZoneId = &zoneId
			req.RecordId = records[idx].RecordId
			if _, err := p.pvtApi.DeletePrivateZoneRecord(req); err != nil {
				log.Errorf("Failed to delete record '%d' in TencentCloud DNS: %v", req.RecordId, err)
				errs = append(errs, err)
			}
		} else {
			log.Errorf("Failed to find %s:%s record with value '%s' to delete", ep.RecordType, ep.DNSName, target)
		}
	}

	return nil
}

func toRecordKey(recordType, domain string, subDomain string) string {
	if subDomain != "" {
		domain = getDNSName(subDomain, domain)
	}
	return fmt.Sprintf("%s:%s", recordType, domain)
}

func toEndpointKey(ep *endpoint.Endpoint) string {
	return fmt.Sprintf("%s:%s", ep.RecordType, ep.DNSName)
}

func resolveTTL[T int64 | uint64](ttl endpoint.TTL, defaultTTL int) T {
	if ttl.IsConfigured() {
		return T(ttl)
	}
	return T(defaultTTL)
}

func getSubDomain(dnsname, domain string) string {
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
		return fmt.Sprintf("%q", value)
	}
	return value
}
