# dispatcher

### http 50300

* /campaign/:subscription_hash
Record campaign access and gain content

Static paths to get the content (probably, need only robots and favicon)
* /static/
* /favicon.ico
* /robots.txt

Metric path (json)
* /debug/vars

Notify about new records and rows changes via this address
* /cqr?t=operator_ip

# contentd:
### RPC 50301

RPC method:
SVC.GetContentByCampaign

Request data, all fields mandatory:
> type GetUrlByCampaignHashParams struct {
>        Msisdn       string
>        CampaignHash string
>        Tid          string
>        CountryCode  int64
>        OperatorCode int64
> }

Response data:
> type ContentSentProperties struct {
>        Msisdn         string `json:"msisdn"`
>        Tid            string `json:"tid"`
>        ContentPath    string `json:"content_path"`
>        CapmaignHash   string `json:"capmaign_hash"`
>        CampaignId     int64  `json:"campaign_id"`
>        ContentId      int64  `json:"content_id"`
>        ServiceId      int64  `json:"service_id"`
>        SubscriptionId int64  `json:"subscription_id"`
>        CountryCode    int64  `json:"country_code"`
>        OperatorCode   int64  `json:"operator_code"`
> }

### HTTP 50302

Metrics:
* /debug/vars

Notify table updates:
* /cqr?t=campaign
* /cqr?t=service
* /cqr?t=content
* /cqr?t=service_content
* /cqr?t=subscriptions
* /cqr?t=content_sent


# qlistener:
###  HTTP 50304

**Metrics**:
* /debug/vars

**Notify table updates**:
* /cqr?t=subscriptions

# mt: 
### HTTP 50305 

**Metrics**:
/debug/vars

**Notify table updates**:
* /cqr?t=blacklist
* /cqr?t=services
* /cqr?t=operators

