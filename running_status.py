import datetime

from flask import Flask
import pandas as pd
import loggingvar
app = Flask(__name__)

import pickle
import json
file = open('connections.pkl', 'rb')
conn = pickle.load(file)

pass_keys = ['57734']

def check_status(x):
    current_time = datetime.datetime.now()
    if 60*60>(current_time - x).seconds>15*60:
        return '*Need to check*'
    elif (current_time - x).seconds>60*60:
        return '!!Stopped!!'
    else:
        return 'Online;)'

def find_json(ipaddress, ScriptName):
    data = []
    # print(conn["ACTIVE_CONNECTIONS"][ipaddress])
    for connkey, connect in conn["ACTIVE_CONNECTIONS"][ipaddress].items():
        # print(connect)
        if connect["info"]['Scriptname'] == ScriptName:
            data.append(connect)
    if len(data)!=0:
        return json.dumps(data, indent=4)
    else:
        return "No such connection found"

def get_status_table():
    def highlight(s):
        if 'online' in s['status'].lower():
            return ['background-color: green'] * len(s)
        elif 'check' in s['status'].lower():
            return ['background-color: yellow'] * len(s)
        else:
            return ['background-color: red'] * len(s)
    with open('connections.pkl', 'rb') as file:
        data = pickle.load(file)
    dfdata = {"IP Addresses": [], 'last_contact': [], 'first_contact': [], 'Connection no.': [], 'info': []}
    for ip in data['ACTIVE_CONNECTIONS'].keys():
        for index, inst in enumerate(data['ACTIVE_CONNECTIONS'][ip].keys()):
            dfdata["IP Addresses"].append(ip)
            dfdata["Connection no."].append(index)
            # print(data['ACTIVE_CONNECTIONS'][ip])
            dfdata["last_contact"].append(data['ACTIVE_CONNECTIONS'][ip][inst]["Last Contact"])
            dfdata['first_contact'].append(data['ACTIVE_CONNECTIONS'][ip][inst]["First Contact"])
            if 'info' in data['ACTIVE_CONNECTIONS'][ip][inst].keys():
                if len(data['ACTIVE_CONNECTIONS'][ip][inst]['info']) > 0:
                    dfdata["info"].append(str(data['ACTIVE_CONNECTIONS'][ip][inst]['info']))
                else:
                    dfdata["info"].append(None)
            else:
                dfdata["info"].append(None)

    # print(dfdata)
    data2check = loggingvar.fields_to_monitor
    fields = [x.split('|')[0] for x in data2check.values()]
    dfdata2 = {x: list() for x in fields}
    dfdata.update(dfdata2)
    for x in dfdata['info']:
        if x is not None and len(x) > 0:
            dat = eval(x)
            for field in fields:
                try:
                    dfdata[field].append(dat[field])
                except KeyError:
                    dfdata[field].append(pd.NA)
        else:
            for x in fields:
                dfdata[x].append(pd.NA)
    del dfdata['info']
    # print(dfdata)
    df = pd.DataFrame(data=dfdata)
    df.style.apply(highlight, axis=1)
    # info_list = df['info'].to_list()
    # for x in
    df['first_contact'] = pd.to_datetime(df['first_contact'], format="%Y-%m-%d %H:%M:%S.%f")
    df['last_contact'] = pd.to_datetime(df['last_contact'], format="%Y-%m-%d %H:%M:%S.%f")
    df['status'] = df['last_contact'].apply(func=check_status)

    df = df.sort_values(by='first_contact', ascending=False)

    return df.to_html()


@app.route('/')
def running_status():
    return 'Api is up and running'

@app.route('/status')
def status_table():
    table = get_status_table()
    return f"""
            <title>Rappi Status</title>
            <h1 style="color:Tomato;">Rappi online crawl status</h1>
            <h2>Refresh time = {datetime.datetime.now()}</h2>
            <body>
            {table}
            </body>
            """

@app.route('/<ipaddress>/<ScriptName>/status')
def status(ipaddress, ScriptName):
    return find_json(ipaddress, ScriptName)
    # return 'yes'

@app.route('/<ipaddress>/<ScriptName>/stop/<key>')
# @app.route('/status')
def stop_script(ipaddress, ScriptName, key):
    if key in pass_keys:
        return '{"key_status":"valid"}'
    else:
        return 'invalid key'




app.run(debug=True)