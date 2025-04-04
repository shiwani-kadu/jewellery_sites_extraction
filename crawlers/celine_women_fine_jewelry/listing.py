import argparse
import json
import logging
import os
import random
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
import requests
from dotenv import load_dotenv
from urllib.parse import urlparse
import time

from parsel import Selector

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)

# Constants
BROWSER_VERSIONS = [
    "chrome99", "chrome100", "chrome101", "chrome104", "chrome107", "chrome110",
    "chrome116", "chrome119", "chrome120", "chrome123",
    "chrome99_android", "edge99", "edge101", "safari15_3", "safari15_5",
    "safari17_0", "safari17_2_ios"
]

HEADERS = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "accept-language": "en-US,en;q=0.9",
        "cache-control": "max-age=0",
        "priority": "u=0, i",
        "sec-ch-ua": "\"Chromium\";v=\"134\", \"Not:A-Brand\";v=\"24\", \"Google Chrome\";v=\"134\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "none",
        "sec-fetch-user": "?1",
        "upgrade-insecure-requests": "1",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
        # "cookie": "dwanonymous_1699f628d3d46e8e56150c85350ac8c8=adEOi8HZh1wma2Ic6NO1LIp97b; __cq_uuid=acI26HlXgdAsTZEDjeay1miK0L; _evga_836c={%22uuid%22:%220f2f7acc043406a3%22}; _sfid_da3c={%22anonymousId%22:%220f2f7acc043406a3%22%2C%22consents%22:[{%22consent%22:{%22provider%22:%22Consent%20Provider%22%2C%22purpose%22:%22Personalization%22%2C%22status%22:%22Opt%20In%22}%2C%22lastUpdateTime%22:%222025-03-19T04:24:18.086Z%22%2C%22lastSentTime%22:%222025-03-19T04:24:18.089Z%22}]}; OptanonAlertBoxClosed=2025-03-19T04:24:18.118Z; _ga=GA1.1.1135380662.1742358260; _gcl_au=1.1.726850238.1742358260; FPID=FPID2.2.jfPumGh2%2BVaUScj39dgohNWmJI1ibmsaS59jMlkUpo4%3D.1742358260; FPAU=1.2.1485818975.1742358256; _cs_c=0; _scid=hYMoftswY3PIyXEN-fq243-Y5PuEE-jJ; _fwb=21knOE8oYA6NwI7PrDohUE.1742358262570; _tt_enable_cookie=1; _ttp=01JPPAAD7YK9E4GPASH21Q0H2G_.tt.1; _fbp=fb.1.1742358263464.3084695962115371; _ScCbts=%5B%5D; _sctr=1%7C1742322600000; dwanonymous_3767c428bc85c24addacb154faebb7e9=abeqRxLGRVjwPReFqnKYPCHLmJ; _evga_778b={%22uuid%22:%220f2f7acc043406a3%22}; dwanonymous_31d77c429f7551bcc1c28a2e42deb934=adf0hCf4b4fkTpN1HtaapNUSMJ; dwanonymous_6801e544c884f5d23c18063d0c42e72b=aby6L5k6najNI5SLwkDR1ME8YM; dwanonymous_e8c6b3ce9ee1148af3a76fb665b9e999=acl2XjsXUmq6cGW71GqRKGaZ8m; dwanonymous_c46cd6d83bbd886f1e5f38bedadee120=aci6rnhHtoEM6mMtOsfxagONTu; dwanonymous_62993b7783836af00b1db1fb9d09161e=abEUJ365hvQphXSC5qBAgi28XX; dwanonymous_f1605329b118d6e2b77937e7f3784880=aewbzE2JlRahRh3b0mEnZAerc1; dwanonymous_2653b16ec5156d348c88a739f9814cf4=beAMajU0kc4Wcc7XRVXKWMbZs0; dwanonymous_72874ff8d7eb566918436a68980c3d75=bdbojmasALrLdFM2Z3yEntsKFV; _evga_5529={%22uuid%22:%220f2f7acc043406a3%22}; __lt__cid=6df54bbd-6dfc-4855-b243-0436b2536b00; _yjsu_yjad=1742367715.5e94ac9c-c37a-482f-98d5-b3b75fed4139; _clck=nnoqox%7C2%7Cfuc%7C0%7C1904; dwanonymous_3b378c362fcf0af76290adb0b4fefd87=acacLZljHrLqWDyDCkqHT8V6Io; _evga_3e15={%22uuid%22:%220f2f7acc043406a3%22}; _evga_ab68={%22uuid%22:%220f2f7acc043406a3%22}; GlobalE_Data=%7B%22countryISO%22%3A%22AE%22%2C%22cultureCode%22%3A%22ar%22%2C%22currencyCode%22%3A%22AED%22%2C%22apiVersion%22%3A%222.1.4%22%7D; GlobalE_CT_Data=%7B%22CUID%22%3A%7B%22id%22%3A%22570104682.308105883.717%22%2C%22expirationDate%22%3A%22Fri%2C%2021%20Mar%202025%2006%3A37%3A49%20GMT%22%7D%2C%22CHKCUID%22%3Anull%2C%22GA4SID%22%3A419661689%2C%22GA4TS%22%3A1742537269185%2C%22Domain%22%3A%22www.celine.com%22%7D; dwac_dd8bb26d284bc3d3b1c4e84925=sjZbQNXEZSJEMDpeiZc2b9Un0Ams9WEQ8BE%3D|dw-only|||USD|false|America%2FNew%5FYork|true; cquid=||; sid=sjZbQNXEZSJEMDpeiZc2b9Un0Ams9WEQ8BE; __cq_dnt=0; dw_dnt=0; dwsid=gjElB6YSTH0MWA4jjLWuleUgrk7fI0hPuHkb7mzkq8_rba18fBPg5LWfUHptPcqejjzjiZXguoKD5ilNgsG6KQ==; bm_mi=3392D3FD135C3E4CB9BB2F64424F6D82~YAAQdv7UF8Ffoa2VAQAAJsk3xxsYCrag4cMuFZDvzX5Fc1cyXqUoUYuERMp/hTA3W3Dpoh3c5LXAwoAeLiG/kcjDVdr0amjnSKAhaJSEfO368OgcOHiJ//3O/r0I+Iiy7NZrcG63SJAuWSmTfd/JdNjtcLgDRWeA6oZlqRCnrPtJvi5Rf/YENe+/T6IsQCU05j3I9DJgTNN2k9pHY2Ic8hOMkIIFH13DYYLtMBAYOUaMi8Ld80aKLs5YJ5uYd9zMT7hLvERdyStLti8mDN98PxpTy6SYzgULZF4q5O5K1EwH7Fl5sRU1cyaWaHhMWf2kTd1B+Ko554uDtiSxPkNMGPelt6dVdTOllDHY2rfHv9qBwcCLo2FD76/gZhoGa8iZVJ6Nobuwq3uBSPzrDw==~1; tfpsi=5b5612b8-88ff-4721-a312-0f64e9ff83bc; FPLC=3X3tfIZ%2BvwA5idcFlJwX%2FQ3d%2BAIeQj1%2BPZjB1pLmY3f4iV4RPhHyej%2BnICAEqcf79xaorAopoFuwMPWmw7M%2F6rNtBo03SxTfxCCDXZVoyOiYKmEG%2FIsx8lxAfH5nww%3D%3D; ak_bmsc=E3C37A703C0146FECCD080FC0F6C1150~000000000000000000000000000000~YAAQRXLBF4Cfm7eVAQAAhdM3xxu3/HDO4NZ7ivMuyK7mb05mIKjGHNg2V54OkysPu5ICaRo2gEcXKcUt2t2p5tZZaP9aAumjRJc8t8BqwwsAyp5WT3h5sRpN/yJuMYAE2/NKjAbDIB3QsrrjGA+EQ/iivIQrFwUy6myWf6MzwDoXoP/sVFD3NYxcv62YFOnz+1a86k3nRynE0I6nCIWxUJGIy20KoacB0L9/cSy33oLoQvFcLo3xE8kzfwZ9aYQHzY7pi6EWU08wCDAh/o3YWEwVIjZGhCpNTuHVUFlAyw2Sjj2OW1y2SDyyyEk3bmhqYYK3wfhPngNGg19WSF2OB5yhdsQY8Lh7gwdbsROUW1JyVY6CBWkuCJy0iZ8yjNs25VJC6fEj5SlQiJ2iBv08ZcYBwEi7gDxgVJG4mu2JKSbCADFO/YjefPLPck/8WGhO4iHILvrZShpnfwsRyn9/B+jbWo9wul7fIclS7B6kP7+Bq0zeXXHCK2vwdzb74tTVpUgCgQ9MjmzpkFKJQsGD2c5zQEmmfoK5ARinSL02NjvsBjv6Af6PoIlQxnj5bA==; dwac_4e82133e2fcfc456e7513e64d7=sjZbQNXEZSJEMDpeiZc2b9Un0Ams9WEQ8BE%3D|dw-only|||GBP|false|Europe%2FLondon|true; dwac_8045cd87323626694dde09ad45=sjZbQNXEZSJEMDpeiZc2b9Un0Ams9WEQ8BE%3D|dw-only|||AUD|false|Australia%2FCanberra|true; AKA_A2=A; ga4_list_data=%5B%7B%22item_id%22%3A%22461IX6SIL.36SI%22%2C%22index%22%3A1%2C%22item_list_name%22%3A%22Category%20%3E%20CELINE%20SHOP%20MEN%20%3E%20JEWELLERY%20%3E%20NEW%22%2C%22item_list_id%22%3A%22Category%22%7D%2C%7B%22item_id%22%3A%22461IY6SIL.36SI%22%2C%22index%22%3A1%2C%22item_list_name%22%3A%22Category%20%3E%20CELINE%20SHOP%20MEN%20%3E%20JEWELLERY%20%3E%20EARRINGS%22%2C%22item_list_id%22%3A%22Category%22%7D%5D; __cq_bc=%7B%22bfml-CELINE_AU%22%3A%5B%7B%22id%22%3A%22461IY6SIL%22%2C%22sku%22%3A%22461IY6SIL.36SI%22%7D%2C%7B%22id%22%3A%22461IX6SIL%22%2C%22type%22%3A%22vgroup%22%2C%22alt_id%22%3A%22461IX6SIL.36SI%22%7D%5D%7D; ABTastySession=mrasn=&lp=https%253A%252F%252Fwww.celine.com%252Fen-us%252Fceline-shop-men%252Fjewellery%252F%253Fnav%253DE005-VIEW-ALL; cqcid=adEOi8HZh1wma2Ic6NO1LIp97b; OptanonConsent=isGpcEnabled=0&datestamp=Mon+Mar+24+2025+14%3A07%3A16+GMT%2B0530+(India+Standard+Time)&version=202501.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=94efd53b-97bc-4ffd-b9cf-c6505c3d261e&interactionCount=1&isAnonUser=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0004%3A1%2CC0005%3A1&intType=1&geolocation=US%3BNJ&AwaitingReconsent=false; wcs_bt=s_2e10aa9fd64:1742805436; _uetsid=0a643370088811f08d49af313b46a905; _uetvid=0846d3b0047a11f085346df1972c41c7; _cs_id=590afe34-73c0-a0bd-eaa3-2b8459405ff8.1742358261.10.1742805436.1742804081.1.1776522261825.1.x; _scid_r=kwMoftswY3PIyXEN-fq243-Y5PuEE-jJtOLDYw; __cq_seg=0~0.00!1~0.00!2~0.00!3~0.00!4~0.00!5~0.00!6~0.00!7~0.00!8~0.00!9~0.00!f0~15~5; FPGSID=1.1742804078.1742805861.G-MENM2C5E1Q.fz2YAqzxad-I7h17X7hCwg; _cs_s=15.0.0.9.1742807668121; bm_sz=D8077F3314A5794EB71E4EA754340271~YAAQpMosF10z3qeVAQAAVh5TxxtNWCWlTzC5070579zXfezZV0irJHeCY+MRmbsQJEa0pgVB6lElSjuAFaNpWitvZyFmEkPsgcMVZNbKrdV9jf6APMfQLb7nDZ7Cqb+pfK9seY2mLU6HVJLPuWfaYzEodWZHmvzUiyElEJ93/kjhaaf5lvy2mxy1lz/vxyl/0nPQ0bagFEpEjoDOiQ2uYeBht5fOzXwOXHj930J/GWngJg079sNNwIgDsVBTV3IUeu4eFD8t4yuAePhht1nOOOoynnUZniJ47JXey2C/joBzilnZn52rTzqdegdSfaKXSSMkO/NCU4LRftzIF5SPNh/qgs++vQZ0Tscl5QIE6hxSn8gMNUlKP/5Q01Ggl1IbynCWjzW9RFM7/dmGP25ANv0pK+KiTspOHvOxLM4i2O1F0PTRj+/iGekn+VGV9k4yqUIDVkRdtgp7QPzJn6V9XJFBbzKKEDUfUAGVOlGM5ZgCtX9oa9l0o9I5MXPmBcmxML4zimeiEKv6DKcn7SD2TzYsB3aHYE3mxQsy0j/6QkrYyNE=~4342071~4273718; _ga_MENM2C5E1Q=GS1.1.1742804081.11.1.1742805873.0.0.768341032; ABTasty=uid=7nydt2mq4trar177&fst=1742358251247&pst=1742537233083&cst=1742804078070&ns=7&pvt=67&pvis=20&th=; bm_sv=485D2D518077FFC86E8600A461C0B35A~YAAQwIXYF5eQvquVAQAAJCJTxxs9LblXJaxeYtq9yEZCgmgDZnK1DRtpWaJ2w5FDGA7WIm4KtUS+dPj+jcu+F7icsRYTzajSwvEKfhHtnYxE1s0pvOUwY0ZkIkLaNSJUamQm5mOBwHtp4K7u6+TDT2J11H8CxzTShk/0+aSx7nLya9T8uKqQKbnW/PVaGja6TOLjrFiaixiHe6KkYA1+PW3tkcHv3yfnbteM+4F9LR8dv4Qh7wetAn7aXZpjhdSAV+E=~1; _abck=F8E0301623B83D1AEC7EDE5166DA4D42~-1~YAAQpMosF40z3qeVAQAAgCVTxw1Y4VqFE/nvYRcIlzamN4Yo52X6DF0dbBS7yRvwg6vCzBWpET2cBzZi9Fw3BhivcaAq+aIeKXReadz8O6E+nBeIUNFYD/9bTxeohx19RkeCM0+P8WpfD/UkuhXyiN+8gF5MftD7ha8IQgMoMwmoes0vh14q77LMKiDD8iujZgboEUkv2bEceHTmmWK3gX38ZYynu384PfYUtzt1zqtZyyvJHjtz3hJgUDkzIQQuc3JSeBOr3V2OdyvWeF9pg8Tx7+NB24TIrndxxqHALAFBX3KrGT5mZSTcyH+Fwk0Zd2XZYFeYvyvKDxW91ayyDuvlmFo8fOoRJYDcOLupLWs04Qi+JlT1sOEtgPVw1y6/UxVHqV86rFtuGZFAjmCvjbTpttjHc8W3dXCCB+LqaydglmGiq7gdVP1IguvjQpJptGkiDBwb+xTLI//46wjIhF8zipZrWWBcc+Ftqf5GL5o4FcVpst20HcbywA4rzvVv4TM86jgI7476j+LNqsoUVWAwek/s87cgK2vU+xN3CCIXhPdvn54M378zFZVzYSVX6Q032qo=~-1~-1~1742807674; inside-us7=129866361-3e7908110188a5caa5d51b570544ce08fca588d8ade2563a4098e7b1852db7ad-5-5; RT=\"z=1&dm=www.celine.com&si=f50746b1-73bf-446c-8356-2791156436a1&ss=m8msjrjf&sl=c&tt=f8d&obo=9&rl=1&ld=12lha&r=3thywyfr&ul=12lhb\""
    }

cookies_cn = {
    'sensorsdata2015jssdkcross': '%7B%22distinct_id%22%3A%22195ad34629b13b2-0c93e93e93e93e8-26011d51-1049088-195ad34629c139e%22%2C%22first_id%22%3A%22%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E7%9B%B4%E6%8E%A5%E6%B5%81%E9%87%8F%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC_%E7%9B%B4%E6%8E%A5%E6%89%93%E5%BC%80%22%2C%22%24latest_referrer%22%3A%22%22%7D%2C%22identities%22%3A%22eyIkaWRlbnRpdHlfY29va2llX2lkIjoiMTk1YWQzNDYyOWIxM2IyLTBjOTNlOTNlOTNlOTNlOC0yNjAxMWQ1MS0xMDQ5MDg4LTE5NWFkMzQ2MjljMTM5ZSJ9%22%2C%22history_login_id%22%3A%7B%22name%22%3A%22%22%2C%22value%22%3A%22%22%7D%2C%22%24device_id%22%3A%22195ad34629b13b2-0c93e93e93e93e8-26011d51-1049088-195ad34629c139e%22%7D',
    'acw_tc': '1a0c380c17429751502851961e00ab3fd447afd701216e11d6e2ceb47ac23a',
    'guest_token': '{%22value%22:%22yG6TjciXENooobtS4uhxSkI37d3XRNoo%22}',
    'PHPSESSID': '071113d9da8a8ec78804feaf78f22f55',
    'tfstk': 'gKhIvrT6dMjQV5si-HLa5Rqu_QNW00O2waa-o4CFyWFd2g3YbXkE4vk5NliuUkKH-UOSfmwLLe0rfLiqovPr4e2JwlosL8PpE8FS2mNrLurE17isDJUrL47nImuATXJ3a_N3Z7K20IRV-2Vuwr7_VTEnWV4J7y3LDKxTZ7K2bIRq-2VomfS9OQg95zzzJ_n8yNFTjrq8wkeRXOUYXuF-27LsWl48wun8wjuG1Pfgb2TLFLy9-yrxJl1RNG4Qpod0f_C-1ys4D2pcw_h_RJGk4jormJk-o-cEPQ1gTqMT6zGkkTZ7hYhU6XK9MR2-6cUtYUWu5Yg-iW2hz_obO0wx9R_RGqFLb5atMUW0bfZE2Xw9uswz6jyY9AJMY8PQl0hnAa1-DVuqtRcWcGEoLrVTPbYAFuwR4gf4cJr5FNwcNPZ25F6lELXpc_TVXO5QpP4a0FT13ewLSPZ25F6lEJUgSZ865t8C.',
    'ssxmod_itna': 'Yq0xgDuD0DnDcDU2xjxBPqeTPqOqqnemx2+bcmuKDs7fDpxBKidDaxQacQRbcGRD53YDOpiekiQORfie8EmEqqKFE0Sp3DU4i82iQA4h3D44GTDt4DTD34DYDixib2DiydDjxGPynXw5tDm4GWCLKDi4D+Cq=DmqG0DDtDiwdDKqGgCk+s+TxD0Txra73t3QrU8lPdLqhe2DqyD0tQxBd5faxy+nFCaGXe1P9xHx=Dzu7DtqXHSdKdx0pBldXxZhhaKGh3tEG3CD3QtGxPW8MxIGxYY4ePn7DeWsDQWGemnDsUxDfonBeKeD',
    'ssxmod_itna2': 'Yq0xgDuD0DnDcDU2xjxBPqeTPqOqqnemx2+bcmqikfq7YODl2GBDj4+6dQFCtKKki+1qA6H2zhLBiYSWCCYhv5veqidxh4guDjqj6Zk7ke0IdL=LHOizBcom=s13nccP+PVMk+hsjQj8utj+dY/CT0eXfXl7qieNeL+Cj=oOWv3ul6sCjuyDuN048UGY1QACDd6f3KA4+xPb3TQW66Cj3tvmbAlI4q2QBACKfYo=WRGezAGUOYAY9c=pKjvxeqKm/bTqQ=4X4jf5TlLRhl9YNC=oVIA2m7aD7QOx7=DeTFP+7+zW7DZOi9DRDiGDD===',
}

headers_cn = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'max-age=0',
    'if-none-match': '"56d7e-shYwfdscrDlGxB1eAAGIMaZgUXk"',
    'priority': 'u=0, i',
    'sec-ch-ua': '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
    # 'cookie': 'sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%22195ad34629b13b2-0c93e93e93e93e8-26011d51-1049088-195ad34629c139e%22%2C%22first_id%22%3A%22%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E7%9B%B4%E6%8E%A5%E6%B5%81%E9%87%8F%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC_%E7%9B%B4%E6%8E%A5%E6%89%93%E5%BC%80%22%2C%22%24latest_referrer%22%3A%22%22%7D%2C%22identities%22%3A%22eyIkaWRlbnRpdHlfY29va2llX2lkIjoiMTk1YWQzNDYyOWIxM2IyLTBjOTNlOTNlOTNlOTNlOC0yNjAxMWQ1MS0xMDQ5MDg4LTE5NWFkMzQ2MjljMTM5ZSJ9%22%2C%22history_login_id%22%3A%7B%22name%22%3A%22%22%2C%22value%22%3A%22%22%7D%2C%22%24device_id%22%3A%22195ad34629b13b2-0c93e93e93e93e8-26011d51-1049088-195ad34629c139e%22%7D; acw_tc=1a0c380c17429751502851961e00ab3fd447afd701216e11d6e2ceb47ac23a; guest_token={%22value%22:%22yG6TjciXENooobtS4uhxSkI37d3XRNoo%22}; PHPSESSID=071113d9da8a8ec78804feaf78f22f55; tfstk=gKhIvrT6dMjQV5si-HLa5Rqu_QNW00O2waa-o4CFyWFd2g3YbXkE4vk5NliuUkKH-UOSfmwLLe0rfLiqovPr4e2JwlosL8PpE8FS2mNrLurE17isDJUrL47nImuATXJ3a_N3Z7K20IRV-2Vuwr7_VTEnWV4J7y3LDKxTZ7K2bIRq-2VomfS9OQg95zzzJ_n8yNFTjrq8wkeRXOUYXuF-27LsWl48wun8wjuG1Pfgb2TLFLy9-yrxJl1RNG4Qpod0f_C-1ys4D2pcw_h_RJGk4jormJk-o-cEPQ1gTqMT6zGkkTZ7hYhU6XK9MR2-6cUtYUWu5Yg-iW2hz_obO0wx9R_RGqFLb5atMUW0bfZE2Xw9uswz6jyY9AJMY8PQl0hnAa1-DVuqtRcWcGEoLrVTPbYAFuwR4gf4cJr5FNwcNPZ25F6lELXpc_TVXO5QpP4a0FT13ewLSPZ25F6lEJUgSZ865t8C.; ssxmod_itna=Yq0xgDuD0DnDcDU2xjxBPqeTPqOqqnemx2+bcmuKDs7fDpxBKidDaxQacQRbcGRD53YDOpiekiQORfie8EmEqqKFE0Sp3DU4i82iQA4h3D44GTDt4DTD34DYDixib2DiydDjxGPynXw5tDm4GWCLKDi4D+Cq=DmqG0DDtDiwdDKqGgCk+s+TxD0Txra73t3QrU8lPdLqhe2DqyD0tQxBd5faxy+nFCaGXe1P9xHx=Dzu7DtqXHSdKdx0pBldXxZhhaKGh3tEG3CD3QtGxPW8MxIGxYY4ePn7DeWsDQWGemnDsUxDfonBeKeD; ssxmod_itna2=Yq0xgDuD0DnDcDU2xjxBPqeTPqOqqnemx2+bcmqikfq7YODl2GBDj4+6dQFCtKKki+1qA6H2zhLBiYSWCCYhv5veqidxh4guDjqj6Zk7ke0IdL=LHOizBcom=s13nccP+PVMk+hsjQj8utj+dY/CT0eXfXl7qieNeL+Cj=oOWv3ul6sCjuyDuN048UGY1QACDd6f3KA4+xPb3TQW66Cj3tvmbAlI4q2QBACKfYo=WRGezAGUOYAY9c=pKjvxeqKm/bTqQ=4X4jf5TlLRhl9YNC=oVIA2m7aD7QOx7=DeTFP+7+zW7DZOi9DRDiGDD===',
}


def load_cookies(region):
    """
    Loads cookies for a specific region from a JSON configuration file.

    Attempts to open and read the cookies file located in the specified
    path ('../../configs/cookies/chanel.json'), then retrieves cookies
    associated with the provided region key. Logs an error and raises
    the exception if there is any issue during file reading or JSON
    parsing.

    Parameters:
        region (str): The region key for which cookies need to be loaded.

    Returns:
        dict: A dictionary containing cookies associated with the
        provided region key.

    Raises:
        Exception: If there is an error during file reading or JSON parsing.
    """
    try:
        with open('../../configs/cookies/celine_women_fine_jewelry.json', 'r') as f:
            cookies = json.load(f)
            return cookies.get(region, {})
    except Exception as e:
        logging.error(f"Error loading cookies for region {region}: {e}")
        raise


def get_base_url(url):
    """
    Parses a given URL and constructs the base URL consisting of its scheme and netloc components.

    This function accepts a full URL, extracts its scheme and netloc components,
    and returns a string representing the base URL. This can be useful for normalizing
    or simplifying URLs for tasks such as comparison, API calls, or other operations requiring
    a consistent base URL representation.

    Args:
        url (str): The full URL to parse.

    Returns:
        str: A string representing the base URL, combining the scheme and netloc.

    Raises:
        None
    """
    parsed_url = urlparse(url)
    return f"{parsed_url.scheme}://{parsed_url.netloc}"


def parse_links(response, data_list, b_url,cookies,headers,impersonate_version):
    """
    Parses product links from a response, appending them to a provided data list.

    This function extracts product links from a provided HTTP response object by parsing JSON
    data and converting HTML content to find specific anchor tags. It constructs full links
    using a base URL and appends them to the given data list. Additionally, it determines
    if there are more pages to process by checking the presence of a "next" key in the JSON data.
    If any errors occur during this process, they will be logged and the function will return False.

    Arguments:
        response (Response): The HTTP response object from which to parse product links.
        data_list (list): The list to which parsed product links will be appended.
        b_url (str): Base URL used to construct full product links.

    Returns:
        bool: True if there is a 'next' page according to the JSON data; otherwise, False.

    Raises:
        No specific exceptions are raised directly, but any exceptions during processing are logged.
    """
    try:
        response_main = Selector(response.text)
        if '.cn' in b_url:
            get_link_category = response_main.xpath('//a[contains(@href,"/celine-men/jewellery-men/")]/@href').getall()
            for cat_link in get_link_category:
                if 'https://www.celine.cn' not in cat_link:
                    cat_link = 'https://www.celine.cn' + cat_link
                response_req = fetch_page(cat_link, cookies, headers, impersonate_version)
                response = Selector(response_req.text)
                links = response.xpath('//div[contains(@class,"product-card")]/a/@href').getall()
                links = [b_url + link for link in links]
                data_list.extend(links)
        else:
            get_link_category = response_main.xpath('//ul[@id="section-E009"]/li/a/@href').getall()[:-1]
            for cat_link in get_link_category:
                if 'https://www.celine.com' not in cat_link:
                    cat_link = 'https://www.celine.com' + cat_link
                if '/categories/' in cat_link:
                    get_link_category = response_main.xpath('//ul[@id="section-E009"]/li/a/following-sibling::ul/li/a/@href').getall()
                    for cat_link in get_link_category:
                        if 'https://www.celine.com' not in cat_link:
                            cat_link = 'https://www.celine.com' + cat_link
                        response_req = fetch_page(cat_link, cookies, headers, impersonate_version)
                        response = Selector(response_req.text)
                        links = response.xpath('//div[@class="m-product-listing"]/a/@href').getall()
                        # links = [b_url + link for link in links]
                        links_list = []
                        for link in links:
                            if 'jewellery' in link:
                                link = b_url + link
                                links_list.append(link)
                        data_list.extend(links_list)
                else:
                    response_req = fetch_page(cat_link,cookies,headers,impersonate_version)
                    response = Selector(response_req.text)
                    links = response.xpath('//div[@class="m-product-listing"]/a/@href').getall()
                    # links = [b_url + link for link in links]
                    links_list = []
                    for link in links:
                        if 'jewellery' in link:
                            link = b_url + link
                            links_list.append(link)
                    data_list.extend(links_list)
        return bool(False)
    except Exception as e:
        logging.error(f"Error parsing links: {e}")
        return False


def fetch_page(url, cookies, headers, impersonate_version):
    """
    Fetches a web page by sending an HTTP GET request with retries, cookies, headers, and an
    optional browser impersonation version. Implements exponential backoff on retry attempts.

    The function makes use of the requests library to send a GET request and retries the
    connection a specified number of times in case of failure, logging each attempt and
    error encountered. If all attempts fail, an error will be logged and the function
    will cease retries. Delays between retries increase exponentially.

    :param url: The URL of the web page to fetch.
    :type url: str
    :param cookies: Cookies to include in the GET request.
    :type cookies: dict
    :param headers: Headers to include in the GET request.
    :type headers: dict
    :param impersonate_version: Optional parameter for browser impersonation.
    :type impersonate_version: str

    :raises Exception: If all retry attempts fail after the maximum number of retries.

    :returns: Response object retrieved from the GET request.
    :rtype: requests.Response
    """
    max_retries = 5
    retry_delay = 1  # Initial delay in seconds
    if '.cn' in url:
        cookies = cookies_cn
        headers = headers_cn
    for attempt in range(max_retries):
        try:
            response = requests.get(
                url,
                cookies=cookies,
                headers=headers,)
                # impersonate=impersonate_version)
            response.raise_for_status()  # Raise HTTP errors
            return response

        except Exception as e:
            logging.error(f"Attempt {attempt + 1} failed for {url}: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)  # Wait before retrying
                retry_delay *= 2  # Exponential backoff
            else:
                logging.error(f"All {max_retries} attempts failed for {url}. Giving up.")


def process_url(base_url, cookies, headers, region):
    """
    Processes a URL, scrapes paginated content, and aggregates it into a data list.

    This function iterates over paginated content of a URL, sends requests using a
    browser impersonation technique, and retrieves the data. It handles the
    pagination by checking for the presence of a next page link in the response. The
    data from each page is added to the `data_list`. The function stops when there
    are no more pages to process or if the response is invalid.

    Args:
        base_url (str): The base URL to which pagination and additional parameters
            are appended.
        token (str): The API token used for authentication with the scraping
            service.
        cookies (dict): A dictionary of cookies to send with the requests.
        headers (dict): A dictionary of HTTP headers to send with the requests.
        region (str): The region for which the request should be made.

    Returns:
        list: A list containing the aggregated data from all processed pages.
    """
    data_list = []
    page = 1
    b_url = get_base_url(base_url)
    while True:
        target_url = urllib.parse.unquote(f"{base_url}")
        if 'celine.cn' in target_url:
            target_url = urllib.parse.unquote(target_url)
        api_url = f"{target_url}"
        impersonate_version = random.choice(BROWSER_VERSIONS)

        response = fetch_page(api_url, cookies, headers, impersonate_version)
        logging.info(f"{target_url}: {response.status_code}")

        if not response:
            break

        next_page = parse_links(response, data_list, b_url,cookies,headers,impersonate_version)
        if not next_page:
            break

        page += 1

    return data_list


def main():
    """
    Main function for scraping product links from Chanel's website based on region. This function
    handles argument parsing, configuration loading, multithreaded URL processing, and saving
    the results to a JSON file. It ensures error handling for each step of the process.

    Args:
        None

    Returns:
        None

    Raises:
        This function may raise and log exceptions in the following scenarios:
        - If the API token is not available in the environment variables.
        - If there is an error loading cookies for the specified region.
        - If the input URLs JSON file cannot be read.
        - If any URL processing task encounters an error during execution.
        - If there is an error while saving the results to the output JSON file.
    """
    parser = argparse.ArgumentParser(description="Scrape product links from Chanel.")
    parser.add_argument("--region", required=True, help="Region code (e.g., 'cn')")
    args = parser.parse_args()

    # Load configuration

    try:
        cookies = load_cookies(args.region)
    except Exception:
        return

    # Read input URLs from JSON file
    try:
        with open(f'../../input_files/celine_women_fine_jewelry_categories.json', 'r') as f:
            input_urls = json.load(f)
    except Exception as e:
        logging.error(f"Error reading input URLs from JSON file: {e}")
        return

    # Multithreading to process URLs concurrently
    # input_urls = input_urls[f'{args.region}']
    all_links = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [
            executor.submit(process_url, url,cookies, HEADERS, args.region)
            for url in input_urls[args.region]
        ]
        for future in futures:
            try:
                links = future.result()
                all_links.extend(links)
            except Exception as e:
                logging.error(f"Error processing URL: {e}")

    # Save results to a JSON file
    try:
        output_filename = f'../../input_files/listing'
        os.makedirs(output_filename, exist_ok=True)
        output_file = f'../../input_files/listing/celine_women_fine_jewelry_links_{args.region}.json'
        with open(output_file, 'w') as f:
            json.dump(all_links, f, indent=4)
        logging.info(f"Saved {len(all_links)} links to {output_file}")
    except Exception as e:
        logging.error(f"Error saving links to file: {e}")


if __name__ == '__main__':
    main()