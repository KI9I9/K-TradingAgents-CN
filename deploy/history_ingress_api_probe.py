"""Authenticated rejection check, only after candidate file hashes are verified.

No redirects, credentials/tokens never printed; two unsupported scheduler requests.
"""
import json
import sys
import urllib.request
import urllib.error

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs):raise ValueError('redirect_refused')

def check(auth):
    opener=urllib.request.build_opener(urllib.request.ProxyHandler({}),NoRedirect())
    def request(path,body,token=None):
        headers={'Content-Type':'application/json'}
        if token:headers['Authorization']='Bearer '+token
        req=urllib.request.Request('http://127.0.0.1:8000'+path,data=json.dumps(body).encode(),headers=headers)
        try:
            with opener.open(req,timeout=15) as response:return response.status,json.load(response)
        except urllib.error.HTTPError as error:
            return error.code,json.load(error)
    code,login=request('/api/auth/login',auth)
    if code!=200 or login.get('success') is not True:raise ValueError('login_failed')
    token=login['data']['access_token'];statuses=[]
    for suffix in ('','?force=true'):
        status,result=request('/api/scheduler/jobs/tushare_historical_sync/trigger'+suffix,{},token)
        if status!=400 or not isinstance(result.get('detail'),str) or 'Tushare 全市场任务尚未开放' not in result['detail']:
            raise ValueError('unexpected_ingress_response')
        statuses.append(status)
    return {'passed':True,'statuses':statuses,'upstream_requests':0}

if __name__=='__main__':
    try:print(json.dumps(check(json.load(sys.stdin)['auth'])))
    except Exception:raise SystemExit('INGRESS_API_CHECK_FAILED: details withheld')
