import json
import urllib.request

from nzshm_common.util import decompress_string

INDEX_URL = "https://nzshm22-static-reports.s3.ap-southeast-2.amazonaws.com/gt-index/gt-index.json"


# from runzi/automation/run_gt_index.py
def get_index_from_s3():
    index_request = urllib.request.Request(INDEX_URL)
    index_str = urllib.request.urlopen(index_request)
    index_comp = index_str.read().decode("utf-8")
    return json.loads(decompress_string(index_comp))


if __name__ == '__main__':

    gt_index = get_index_from_s3()
    print(gt_index)
