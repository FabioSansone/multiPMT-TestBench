import json

info = {'PMT serrial' : 'KM8010'}

with open('/swgo/Test/SWGO_Testbench/multiPMT-TestBench/server/info.json', 'w', newline='') as f:
    json.dump(info, f, indent=1)