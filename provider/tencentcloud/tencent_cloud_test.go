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
	"fmt"
	"math/rand"
	"slices"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common"
	dnspod "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/dnspod/v20210323"
	privatedns "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/privatedns/v20201028"

	"sigs.k8s.io/external-dns/endpoint"
	"sigs.k8s.io/external-dns/internal/testutils"
	"sigs.k8s.io/external-dns/plan"
	"sigs.k8s.io/external-dns/provider"
)

type MockDNSPodAPI struct {
	counter int64
	config  tencentCloudConfig
	domains []*dnspod.DomainListItem
	records map[string][]*dnspod.RecordListItem
}

type MockPrivateDNSAPI struct {
	counter int64
	config  tencentCloudConfig
	zones   []*privatedns.PrivateZone
	records map[string][]*privatedns.PrivateZoneRecord
}

func (api *MockDNSPodAPI) CreateRecord(request *dnspod.CreateRecordRequest) (*dnspod.CreateRecordResponse, error) {
	if request == nil || request.DomainId == nil {
		return dnspod.NewCreateRecordResponse(), fmt.Errorf("invalid request")
	}

	domainId := strconv.FormatUint(*request.DomainId, 10)
	if _, exist := api.records[domainId]; !exist {
		return dnspod.NewCreateRecordResponse(), fmt.Errorf("domain not found")
	}
	ttl := request.TTL
	if ttl == nil || *ttl <= 0 {
		ttl = common.Uint64Ptr(defaultDnsTTL)
	}
	recordId := common.Uint64Ptr(rand.Uint64())
	value := common.StringPtr(unwrapQuotes(*request.RecordType, *request.Value))
	api.records[domainId] = append(api.records[domainId], &dnspod.RecordListItem{
		RecordId: recordId,
		TTL:      ttl,
		Value:    value,
		Name:     request.SubDomain,
		Line:     request.RecordLine,
		LineId:   request.RecordLineId,
		Type:     request.RecordType,
	})

	response := dnspod.NewCreateRecordResponse()
	response.Response = &dnspod.CreateRecordResponseParams{
		RecordId: recordId,
	}
	return response, nil
}

func (api *MockDNSPodAPI) DeleteRecord(request *dnspod.DeleteRecordRequest) (*dnspod.DeleteRecordResponse, error) {
	if request == nil || request.DomainId == nil || request.RecordId == nil {
		return dnspod.NewDeleteRecordResponse(), fmt.Errorf("invalid request")
	}

	domainId := strconv.FormatUint(*request.DomainId, 10)

	if _, exist := api.records[domainId]; !exist {
		return dnspod.NewDeleteRecordResponse(), fmt.Errorf("domain not found")
	}

	recordId := *request.RecordId
	records := api.records[domainId]

	api.records[domainId] = slices.DeleteFunc(records, func(item *dnspod.RecordListItem) bool {
		return *item.RecordId == recordId
	})

	response := dnspod.NewDeleteRecordResponse()
	response.Response = &dnspod.DeleteRecordResponseParams{}
	return response, nil
}

func (api *MockDNSPodAPI) ModifyRecord(request *dnspod.ModifyRecordRequest) (*dnspod.ModifyRecordResponse, error) {
	if request == nil || request.DomainId == nil || request.RecordId == nil {
		return dnspod.NewModifyRecordResponse(), fmt.Errorf("invalid request")
	}

	domainId := strconv.FormatUint(*request.DomainId, 10)

	if _, exist := api.records[domainId]; !exist {
		return dnspod.NewModifyRecordResponse(), fmt.Errorf("domain not found")
	}

	recordId := *request.RecordId
	records := api.records[domainId]
	idx := slices.IndexFunc(records, func(item *dnspod.RecordListItem) bool {
		return *item.RecordId == recordId
	})
	if idx != -1 {
		if request.Value != nil {
			records[idx].Value = common.StringPtr(unwrapQuotes(*records[idx].Type, *request.Value))
		}
		if request.TTL != nil && *request.TTL > 0 {
			records[idx].TTL = request.TTL
		}
	}

	response := dnspod.NewModifyRecordResponse()
	response.Response = &dnspod.ModifyRecordResponseParams{}
	return response, nil
}

func (api *MockDNSPodAPI) DescribeDomainList(request *dnspod.DescribeDomainListRequest) (*dnspod.DescribeDomainListResponse, error) {
	response := dnspod.NewDescribeDomainListResponse()
	response.Response = &dnspod.DescribeDomainListResponseParams{
		DomainCountInfo: &dnspod.DomainCountInfo{
			AllTotal: common.Uint64Ptr(uint64(len(api.domains))),
		},
		DomainList: api.domains,
	}
	return response, nil
}

func (api *MockDNSPodAPI) DescribeRecordList(request *dnspod.DescribeRecordListRequest) (*dnspod.DescribeRecordListResponse, error) {
	domainId := ""
	if request.Domain != nil {
		idx := slices.IndexFunc(api.domains, func(item *dnspod.DomainListItem) bool {
			return *item.Name == *request.Domain
		})
		if idx != -1 {
			domainId = strconv.FormatUint(*api.domains[idx].DomainId, 10)
		}
	} else if request.DomainId != nil {
		domainId = strconv.FormatUint(*request.DomainId, 10)
	}

	if domainId == "" {
		return dnspod.NewDescribeRecordListResponse(), nil
	}

	records := api.records[domainId]
	response := dnspod.NewDescribeRecordListResponse()
	response.Response = &dnspod.DescribeRecordListResponseParams{
		RecordCountInfo: &dnspod.RecordCountInfo{
			TotalCount: common.Uint64Ptr(uint64(len(records))),
		},
		RecordList: records,
	}
	return response, nil
}

func (api *MockDNSPodAPI) nextID() int64 {
	id := atomic.AddInt64(&api.counter, 1)
	return id
}

func (api *MockDNSPodAPI) newRecord(ep *endpoint.Endpoint, target, domain string) *dnspod.RecordListItem {
	subname := getSubDomain(ep.DNSName, domain)
	value := unwrapQuotes(ep.RecordType, target)
	recordId := uint64(api.nextID())
	ttl := uint64(ep.RecordTTL)

	if ttl <= 0 {
		ttl = uint64(defaultDnsTTL)
	}

	return &dnspod.RecordListItem{
		// DomainId:   &domainId,
		RecordId: &recordId,
		Type:     &ep.RecordType,
		Name:     &subname,
		TTL:      &ttl,
		Value:    &value,
	}
}

func (api *MockDNSPodAPI) resetData(data *map[string][]*endpoint.Endpoint) {
	domains := make([]*dnspod.DomainListItem, 0)
	dnsRecords := make(map[string][]*dnspod.RecordListItem, 0)
	for domain, endpoints := range *data {
		domainId := rand.Uint64()
		zoneId := strconv.FormatUint(domainId, 10)
		domains = append(domains, &dnspod.DomainListItem{
			Name:     &domain,
			DomainId: &domainId,
		})
		dnsRecords[zoneId] = make([]*dnspod.RecordListItem, 0)
		for _, ep := range endpoints {
			for _, target := range ep.Targets {
				dnsRecords[zoneId] = append(dnsRecords[zoneId], api.newRecord(ep, target, domain))
			}
		}
	}
	api.domains = domains
	api.records = dnsRecords
}

func (api *MockPrivateDNSAPI) CreatePrivateZoneRecord(request *privatedns.CreatePrivateZoneRecordRequest) (*privatedns.CreatePrivateZoneRecordResponse, error) {
	if request.ZoneId == nil {
		return privatedns.NewCreatePrivateZoneRecordResponse(), fmt.Errorf("invalid request")
	}
	zoneID := *request.ZoneId
	if _, exist := api.records[zoneID]; !exist {
		api.records[zoneID] = make([]*privatedns.PrivateZoneRecord, 0)
	}
	ttl := request.TTL
	if ttl == nil {
		ttl = common.Int64Ptr(defaultPvtTTL)
	}
	recordId := common.StringPtr(uuid.NewString())
	recordValue := common.StringPtr(unwrapQuotes(*request.RecordType, *request.RecordValue))
	api.records[zoneID] = append(api.records[zoneID], &privatedns.PrivateZoneRecord{
		RecordId:    recordId,
		ZoneId:      &zoneID,
		RecordValue: recordValue,
		SubDomain:   request.SubDomain,
		RecordType:  request.RecordType,
		TTL:         ttl,
		MX:          request.MX,
		Weight:      request.Weight,
	})
	response := privatedns.NewCreatePrivateZoneRecordResponse()
	response.Response = &privatedns.CreatePrivateZoneRecordResponseParams{
		RecordId: recordId,
	}

	return response, nil
}

func (api *MockPrivateDNSAPI) DeletePrivateZoneRecord(request *privatedns.DeletePrivateZoneRecordRequest) (*privatedns.DeletePrivateZoneRecordResponse, error) {
	if request.ZoneId == nil {
		return privatedns.NewDeletePrivateZoneRecordResponse(), nil
	}

	recordIDs := make(map[string]struct{}, len(request.RecordIdSet)+1)
	if request.RecordId != nil {
		recordIDs[*request.RecordId] = struct{}{}
	}
	for _, recordID := range request.RecordIdSet {
		recordIDs[*recordID] = struct{}{}
	}

	records := api.records[*request.ZoneId]
	remaining := make([]*privatedns.PrivateZoneRecord, 0, len(records))
	for _, record := range records {
		if _, deleteRecord := recordIDs[*record.RecordId]; !deleteRecord {
			remaining = append(remaining, record)
		}
	}
	api.records[*request.ZoneId] = remaining

	response := privatedns.NewDeletePrivateZoneRecordResponse()
	response.Response = &privatedns.DeletePrivateZoneRecordResponseParams{}
	return response, nil
}

func (api *MockPrivateDNSAPI) ModifyPrivateZoneRecord(request *privatedns.ModifyPrivateZoneRecordRequest) (*privatedns.ModifyPrivateZoneRecordResponse, error) {
	if request == nil || request.ZoneId == nil || request.RecordId == nil {
		return privatedns.NewModifyPrivateZoneRecordResponse(), fmt.Errorf("invalid request")
	}
	records := api.records[*request.ZoneId]
	idx := slices.IndexFunc(records, func(item *privatedns.PrivateZoneRecord) bool {
		return *item.RecordId == *request.RecordId
	})
	if idx != -1 {
		if request.RecordValue != nil {
			records[idx].RecordValue = common.StringPtr(unwrapQuotes(*records[idx].RecordType, *request.RecordValue))
		}
		if request.TTL != nil && *request.TTL > 0 {
			records[idx].TTL = request.TTL
		}
	}
	return privatedns.NewModifyPrivateZoneRecordResponse(), nil
}

func (api *MockPrivateDNSAPI) DescribePrivateZoneList(request *privatedns.DescribePrivateZoneListRequest) (*privatedns.DescribePrivateZoneListResponse, error) {
	response := privatedns.NewDescribePrivateZoneListResponse()
	response.Response = &privatedns.DescribePrivateZoneListResponseParams{
		TotalCount:     common.Int64Ptr(int64(len(api.zones))),
		PrivateZoneSet: api.zones,
	}
	return response, nil
}

func (api *MockPrivateDNSAPI) DescribePrivateZoneRecordList(request *privatedns.DescribePrivateZoneRecordListRequest) (*privatedns.DescribePrivateZoneRecordListResponse, error) {
	records := api.records[*request.ZoneId]
	response := privatedns.NewDescribePrivateZoneRecordListResponse()
	response.Response = &privatedns.DescribePrivateZoneRecordListResponseParams{
		TotalCount: common.Int64Ptr(int64(len(records))),
		RecordSet:  records,
	}
	return response, nil
}

func (api *MockPrivateDNSAPI) nextID() string {
	id := atomic.AddInt64(&api.counter, 1)
	return strconv.FormatInt(id, 10)
}

func (api *MockPrivateDNSAPI) newRecord(ep *endpoint.Endpoint, target, domain, zoneId string) *privatedns.PrivateZoneRecord {
	subname := getSubDomain(ep.DNSName, domain)
	value := unwrapQuotes(ep.RecordType, target)
	recordId := api.nextID()
	ttl := int64(ep.RecordTTL)

	if ttl <= 0 {
		ttl = int64(defaultPvtTTL)
	}

	return &privatedns.PrivateZoneRecord{
		ZoneId:      &zoneId,
		RecordId:    &recordId,
		RecordType:  &ep.RecordType,
		TTL:         &ttl,
		SubDomain:   &subname,
		RecordValue: &value,
	}
}

func (api *MockPrivateDNSAPI) resetData(data *map[string][]*endpoint.Endpoint) {
	zones := make([]*privatedns.PrivateZone, 0)
	pvtRecords := make(map[string][]*privatedns.PrivateZoneRecord, 0)
	for domain, endpoints := range *data {
		zoneId := uuid.NewString()
		zones = append(zones, &privatedns.PrivateZone{
			ZoneId: &zoneId,
			Domain: &domain,
			VpcSet: []*privatedns.VpcInfo{{
				UniqVpcId: &api.config.VPCId,
				Region:    &api.config.RegionId,
			}},
		})
		pvtRecords[zoneId] = make([]*privatedns.PrivateZoneRecord, 0)
		for _, ep := range endpoints {
			for _, target := range ep.Targets {
				pvtRecords[zoneId] = append(pvtRecords[zoneId], api.newRecord(ep, target, domain, zoneId))
			}
		}
	}
	api.zones = zones
	api.records = pvtRecords
}

func (p *TencentCloudProvider) resetApiData(data *map[string][]*endpoint.Endpoint) {
	if data != nil {
		if p.privateZone {
			if api, ok := p.pvtApi.(*MockPrivateDNSAPI); ok {
				api.resetData(data)
			}
		} else if api, ok := p.dnsApi.(*MockDNSPodAPI); ok {
			api.resetData(data)
		}
	}
}

func newMockTencentCloudProvider(private bool) *TencentCloudProvider {
	domain := "external-dns-test.com"
	return newMockTencentCloudProviderWithConfig(
		endpoint.NewDomainFilter([]string{domain}),
		provider.NewZoneIDFilter([]string{}),
		private, map[string][]*endpoint.Endpoint{
			domain: createEndpoints("nginx", domain),
		},
	)
}

func newMockTencentCloudProviderWithConfig(domainFilter *endpoint.DomainFilter, zoneIDFilter provider.ZoneIDFilter, privateZone bool, endpointsMap map[string][]*endpoint.Endpoint) *TencentCloudProvider {
	cfg := tencentCloudConfig{
		RegionId: "ap-shanghai",
		VPCId:    "vpc-abcdefg",
	}

	dnsApi := &MockDNSPodAPI{
		counter: 0,
		config:  cfg,
		domains: make([]*dnspod.DomainListItem, 0),
		records: make(map[string][]*dnspod.RecordListItem, 0),
	}
	pvtApi := &MockPrivateDNSAPI{
		counter: 0,
		config:  cfg,
		zones:   make([]*privatedns.PrivateZone, 0),
		records: make(map[string][]*privatedns.PrivateZoneRecord, 0),
	}

	if privateZone {
		pvtApi.resetData(&endpointsMap)
	} else {
		dnsApi.resetData(&endpointsMap)
	}

	tencentCloudProvider := &TencentCloudProvider{
		domainFilter: domainFilter,
		zoneIDFilter: &zoneIDFilter,
		dryRun:       false,
		dnsApi:       dnsApi,
		pvtApi:       pvtApi,
		vpcID:        cfg.VPCId,
		privateZone:  privateZone,
	}

	return tencentCloudProvider
}

func createEndpoints(subname, domain string) []*endpoint.Endpoint {
	dnsName := getDNSName(subname, domain)
	endpoints := []*endpoint.Endpoint{
		endpoint.NewEndpointWithTTL(dnsName, "A", 300, "10.10.10.10"),
		endpoint.NewEndpointWithTTL(dnsName, "TXT", 300, "\"heritage=external-dns,external-dns/owner=default\""),
	}

	return endpoints
}

func TestTencentCloudProvider_PrivateDNS_Records(t *testing.T) {
	p := newMockTencentCloudProvider(true)
	endpoints, err := p.Records(context.Background())

	require.NoError(t, err, "Failed to get records: %v", err)
	assert.Len(t, endpoints, 2, "Incorrect number of records: %d", len(endpoints))
}

func TestTencentCloudProvider_PrivateDNS_ApplyChanges(t *testing.T) {
	domain := "external-dns-test.com"
	defaultEndpoints := createEndpoints("nginx", domain)
	endpointsMap := map[string][]*endpoint.Endpoint{
		domain: defaultEndpoints,
	}
	p := newMockTencentCloudProviderWithConfig(
		endpoint.NewDomainFilter([]string{domain}),
		provider.NewZoneIDFilter([]string{}),
		true, endpointsMap,
	)

	// Test for Create、UpdateOld、UpdateNew、Delete
	// The base record will be created.
	changes := &plan.Changes{
		Create: []*endpoint.Endpoint{
			endpoint.NewEndpointWithTTL("redis.external-dns-test.com", "A", 300, "4.3.2.1"),
		},
		UpdateOld: []*endpoint.Endpoint{
			endpoint.NewEndpointWithTTL("nginx.external-dns-test.com", "A", 300, "10.10.10.10"),
		},
		UpdateNew: []*endpoint.Endpoint{
			endpoint.NewEndpointWithTTL("nginx.external-dns-test.com", "A", 500, "10.10.10.10", "8.8.8.8"),
		},
		Delete: []*endpoint.Endpoint{
			endpoint.NewEndpoint("nginx.external-dns-test.com", "TXT", "\"heritage=external-dns,external-dns/owner=default\""),
		},
	}
	err := p.ApplyChanges(context.Background(), changes)
	require.NoError(t, err, "Failed to apply changes: %v", err)

	endpoints, err := p.Records(context.Background())
	require.NoError(t, err, "Failed to get records: %v", err)
	changedEndpoints := append([]*endpoint.Endpoint{
		endpoint.NewEndpointWithTTL("nginx.external-dns-test.com", "A", 500, "10.10.10.10", "8.8.8.8"),
	}, changes.Create...)
	require.Len(t, endpoints, 2, "Incorrect number of records: %d", len(endpoints))
	assert.True(t, testutils.SameEndpoints(changedEndpoints, endpoints), "expected and actual endpoints don't match. %s:%s", changedEndpoints, endpoints)

	// Test for Delete one target
	p.resetApiData(&endpointsMap)
	changes = &plan.Changes{
		Delete: []*endpoint.Endpoint{
			endpoint.NewEndpointWithTTL("nginx.external-dns-test.com", "A", 300, "10.10.10.10"),
		},
	}
	err = p.ApplyChanges(context.Background(), changes)
	require.NoError(t, err, "Failed to apply changes: %v", err)
	endpoints, err = p.Records(context.Background())
	changedEndpoints = []*endpoint.Endpoint{
		endpoint.NewEndpointWithTTL("nginx.external-dns-test.com", "TXT", 300, "\"heritage=external-dns,external-dns/owner=default\""),
	}
	require.NoError(t, err, "Failed to get records: %v", err)
	require.Len(t, endpoints, 1, "Incorrect number of records: %d", len(endpoints))
	assert.True(t, testutils.SameEndpoints(changedEndpoints, endpoints), "expected and actual endpoints don't match. %s:%s", changedEndpoints, endpoints)

	// Test for Delete another target
	p.resetApiData(&endpointsMap)
	changes = &plan.Changes{
		Delete: []*endpoint.Endpoint{
			endpoint.NewEndpointWithTTL("tencent.external-dns-test.com", "A", 600, "1.2.3.4"),
		},
	}
	err = p.ApplyChanges(context.Background(), changes)
	require.NoError(t, err, "Failed to apply changes: %v", err)
	endpoints, err = p.Records(context.Background())
	require.NoError(t, err, "Failed to get records: %v", err)
	require.Len(t, endpoints, 2, "Incorrect number of records: %d", len(endpoints))
	assert.True(t, testutils.SameEndpoints(defaultEndpoints, endpoints), "expected and actual endpoints don't match. %s:%s", defaultEndpoints, endpoints)

	// Test for Create new records
	p.resetApiData(&endpointsMap)
	changes = &plan.Changes{
		Create: []*endpoint.Endpoint{
			endpoint.NewEndpointWithTTL("new.external-dns-test.com", "A", 300, "5.6.7.8"),
			endpoint.NewEndpointWithTTL("new.external-dns-test.com", "TXT", 300, "\"heritage=external-dns,external-dns/owner=default\""),
		},
	}
	err = p.ApplyChanges(context.Background(), changes)
	require.NoError(t, err, "Failed to apply changes: %v", err)
	endpoints, err = p.Records(context.Background())
	require.NoError(t, err, "Failed to get records: %v", err)
	changedEndpoints = append(defaultEndpoints, changes.Create...)
	require.Len(t, endpoints, 4, "Incorrect number of records: %d", len(endpoints))
	assert.True(t, testutils.SameEndpoints(changedEndpoints, endpoints), "expected and actual endpoints don't match. %s:%s", changedEndpoints, endpoints)

	// Test for Delete new records
	p.resetApiData(&endpointsMap)
	changes = &plan.Changes{
		Delete: []*endpoint.Endpoint{
			endpoint.NewEndpointWithTTL("new.external-dns-test.com", "A", 300, "5.6.7.8"),
			endpoint.NewEndpointWithTTL("new.external-dns-test.com", "TXT", 300, "\"heritage=external-dns,external-dns/owner=default\""),
		},
	}
	err = p.ApplyChanges(context.Background(), changes)
	require.NoError(t, err, "Failed to apply changes: %v", err)
	endpoints, err = p.Records(context.Background())
	require.NoError(t, err, "Failed to get records: %v", err)
	require.Len(t, endpoints, 2, "Incorrect number of records: %d", len(endpoints))
	assert.True(t, testutils.SameEndpoints(defaultEndpoints, endpoints), "expected and actual endpoints don't match. %s:%s", defaultEndpoints, endpoints)
}

func TestTencentCloudProvider_PublicDNS_Records(t *testing.T) {
	p := newMockTencentCloudProvider(false)
	endpoints, err := p.Records(context.Background())
	require.NoError(t, err, "Failed to get records: %v", err)
	assert.Len(t, endpoints, 2, "Incorrect number of records: %d", len(endpoints))
}

func TestTencentCloudProvider_PublicDNS_ApplyChanges(t *testing.T) {
	domain := "external-dns-test.com"
	defaultEndpoints := createEndpoints("nginx", domain)
	endpointsMap := map[string][]*endpoint.Endpoint{
		domain: defaultEndpoints,
	}
	p := newMockTencentCloudProviderWithConfig(
		endpoint.NewDomainFilter([]string{domain}),
		provider.NewZoneIDFilter([]string{}),
		false, endpointsMap,
	)

	// Test for Create、UpdateOld、UpdateNew、Delete
	// The base record will be created.
	changes := &plan.Changes{
		Create: []*endpoint.Endpoint{
			endpoint.NewEndpointWithTTL("redis.external-dns-test.com", "A", 300, "4.3.2.1"),
		},
		UpdateOld: []*endpoint.Endpoint{
			endpoint.NewEndpointWithTTL("nginx.external-dns-test.com", "A", 300, "10.10.10.10"),
		},
		UpdateNew: []*endpoint.Endpoint{
			endpoint.NewEndpointWithTTL("nginx.external-dns-test.com", "A", 500, "10.10.10.10", "8.8.8.8"),
		},
		Delete: []*endpoint.Endpoint{
			endpoint.NewEndpoint("nginx.external-dns-test.com", "TXT", "\"heritage=external-dns,external-dns/owner=default\""),
		},
	}
	err := p.ApplyChanges(context.Background(), changes)
	require.NoError(t, err, "Failed to apply changes: %v", err)

	endpoints, err := p.Records(context.Background())
	require.NoError(t, err, "Failed to get records: %v", err)
	changedEndpoints := append([]*endpoint.Endpoint{
		endpoint.NewEndpointWithTTL("nginx.external-dns-test.com", "A", 500, "10.10.10.10", "8.8.8.8"),
	}, changes.Create...)
	require.Len(t, endpoints, 2, "Incorrect number of records: %d", len(endpoints))
	assert.True(t, testutils.SameEndpoints(changedEndpoints, endpoints), "expected and actual endpoints don't match. %s:%s", changedEndpoints, endpoints)

	// Test for Delete one target
	p.resetApiData(&endpointsMap)
	changes = &plan.Changes{
		Delete: []*endpoint.Endpoint{
			endpoint.NewEndpointWithTTL("nginx.external-dns-test.com", "A", 300, "10.10.10.10"),
		},
	}
	err = p.ApplyChanges(context.Background(), changes)
	require.NoError(t, err, "Failed to apply changes: %v", err)
	endpoints, err = p.Records(context.Background())
	changedEndpoints = []*endpoint.Endpoint{
		endpoint.NewEndpointWithTTL("nginx.external-dns-test.com", "TXT", 300, "\"heritage=external-dns,external-dns/owner=default\""),
	}
	require.NoError(t, err, "Failed to get records: %v", err)
	require.Len(t, endpoints, 1, "Incorrect number of records: %d", len(endpoints))
	assert.True(t, testutils.SameEndpoints(changedEndpoints, endpoints), "expected and actual endpoints don't match. %s:%s", changedEndpoints, endpoints)

	// Test for Delete another target
	p.resetApiData(&endpointsMap)
	changes = &plan.Changes{
		Delete: []*endpoint.Endpoint{
			endpoint.NewEndpointWithTTL("tencent.external-dns-test.com", "A", 600, "1.2.3.4"),
		},
	}
	err = p.ApplyChanges(context.Background(), changes)
	require.NoError(t, err, "Failed to apply changes: %v", err)
	endpoints, err = p.Records(context.Background())
	require.NoError(t, err, "Failed to get records: %v", err)
	require.Len(t, endpoints, 2, "Incorrect number of records: %d", len(endpoints))
	assert.True(t, testutils.SameEndpoints(defaultEndpoints, endpoints), "expected and actual endpoints don't match. %s:%s", defaultEndpoints, endpoints)

	// Test for Create new records
	p.resetApiData(&endpointsMap)
	changes = &plan.Changes{
		Create: []*endpoint.Endpoint{
			endpoint.NewEndpointWithTTL("new.external-dns-test.com", "A", 300, "5.6.7.8"),
			endpoint.NewEndpointWithTTL("new.external-dns-test.com", "TXT", 300, "\"heritage=external-dns,external-dns/owner=default\""),
		},
	}
	err = p.ApplyChanges(context.Background(), changes)
	require.NoError(t, err, "Failed to apply changes: %v", err)
	endpoints, err = p.Records(context.Background())
	require.NoError(t, err, "Failed to get records: %v", err)
	changedEndpoints = append(defaultEndpoints, changes.Create...)
	require.Len(t, endpoints, 4, "Incorrect number of records: %d", len(endpoints))
	assert.True(t, testutils.SameEndpoints(changedEndpoints, endpoints), "expected and actual endpoints don't match. %s:%s", changedEndpoints, endpoints)

	// Test for Delete new records
	p.resetApiData(&endpointsMap)
	changes = &plan.Changes{
		Delete: []*endpoint.Endpoint{
			endpoint.NewEndpointWithTTL("new.external-dns-test.com", "A", 300, "5.6.7.8"),
			endpoint.NewEndpointWithTTL("new.external-dns-test.com", "TXT", 300, "\"heritage=external-dns,external-dns/owner=default\""),
		},
	}
	err = p.ApplyChanges(context.Background(), changes)
	require.NoError(t, err, "Failed to apply changes: %v", err)
	endpoints, err = p.Records(context.Background())
	require.NoError(t, err, "Failed to get records: %v", err)
	require.Len(t, endpoints, 2, "Incorrect number of records: %d", len(endpoints))
	assert.True(t, testutils.SameEndpoints(defaultEndpoints, endpoints), "expected and actual endpoints don't match. %s:%s", defaultEndpoints, endpoints)
}
