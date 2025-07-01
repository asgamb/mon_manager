#from kafkaConnections import kafkaConnections
import sys
import time
import argparse
import subprocess
import os
import pandas as pd

from kafkaConnections import kafkaConnections

from flask import Flask
from flask_restplus import Resource, Api
from threading import Thread
from threading import Event

# Flask and Flask-RestPlus configuration
app = Flask(__name__)
api = Api(app, version='1.0', title='Monitoring Manager API',
          description='Rest API to start and stop monitoring jobs.\nAuthor: Andrea Sgambelluri')
monitoring = api.namespace('MonitoringManager', description='monManger')

portx = 6005
events = {}
threads = {}
jobs = {}
thread_id = 1
infra_polling = 20

deploy_client = 0
# Define the parser
parser = argparse.ArgumentParser()

parser.add_argument('--file', action="store", dest='file', default=0)
args = parser.parse_args()
fx = args.file

#ec = kafkaConnections("configP1.conf")
ec = kafkaConnections(fx)


IDLE_THRESHOLD = 1  # in millicore (1m)
IDLE_COUNTERS = {}  # pod_name -> idle_since_timestamp

period = 5

client_period = 60

save_data = 1


num_client = 0
mess = ""

file_csv = "data/data.csv"

def init_csv():
    global file_csv
    if os.path.isfile(file_csv):
        [root, end] = file_csv.split('.')
        for i in range(0, 100):
           file_csv = root + "_" + str(i) + "." + end
           if not os.path.isfile(file_csv):
               break
    print("save data on file {}".format(file_csv))
    csv_file = open(file_csv, "w")
    csv_file.write("timestamp;cpu;ram;clients\n")
    csv_file.close()


def savedata(string):
    global file_csv
    csv_file = open(file_csv, "a")
    string = string[:-1] + "\n"
    csv_file.write(string)
    csv_file.close()



def run_client():
    global num_client
    num_client = num_client + 1
    cmd = f"bash /home/acc/monitoring/startClient.sh {num_client}"
    #cmd = f"./startClient.sh {num_client}"
    
    try:
        #["python3", "script_da_eseguire.py"],  # comando da eseguire
        result = subprocess.run(
           cmd.split(),
           stdout=subprocess.PIPE,                # cattura stdout
           stderr=subprocess.PIPE,                # cattura stderr
           text=True                              # output in formato stringa (non byte)
        )
        #print("Output standard:")
        #print(result.stdout)
        #print("Errori (stderr):")
        #print(result.stderr)
        #print(f"Codice di ritorno: {result.returncode}")
    except Exception as e:
        print(f"Errore durante l'esecuzione: {e}")


def get_cpu_millicore(cpu_str):
    if cpu_str.endswith("m"):
        return int(cpu_str[:-1])
    return int(float(cpu_str) * 1000)

def get_pod_cpu(namespace):
    result = subprocess.run(
        #["kubectl", "top", "pods", "--no-headers", "--all-namespaces"],
        ["kubectl", "top", "pods","--no-headers",  "-n", namespace],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    now = time.time() * 1000
    lines = result.stdout.strip().splitlines()
    num_lines = len(lines)
    cpu_agg = 0
    mem_agg = 0
    for line in lines:
        #ns, pod, cpu, *_ = line.split()
        pod, cpu, mem = line.split()
        cpu_m = int(get_cpu_millicore(cpu))
        #key = f"{ns}/{pod}"
        mem1, rest = mem.split('M')
        #print(mem1)
        mem_int = int(mem1)
        cpu_agg = cpu_agg + cpu_m
        mem_agg = mem_agg + mem_int
        key = f"{pod}"
        print(f"{pod}-{cpu_m}")
        if cpu_m <= IDLE_THRESHOLD:
            if key not in IDLE_COUNTERS:
                IDLE_COUNTERS[key] = now  # prima volta che è idle
            idle_for = now - IDLE_COUNTERS[key]
            #print(f"{key} - IDLE for {int(idle_for)}s (CPU: {cpu})")
        else:
            if key in IDLE_COUNTERS:
                del IDLE_COUNTERS[key]
            #print(f"{key} - ACTIVE (CPU: {cpu})")
        #print(f"{now};{cpu};{mem1}")
    
    print(f"{now};{cpu_agg/num_lines};{mem_agg/num_lines}")
    return f"{now};{cpu_agg/num_lines};{mem_agg/num_lines}"


def delivery_callback(err, msg):
    if err:
        sys.stderr.write('%% Message failed delivery: %s\n' % err)
    else:
        sys.stderr.write('%% Message delivered to %s [%d] @ %d\n' %
                         (msg.topic(), msg.partition(), msg.offset()))

def infra_thread():
    global thread_id
    event = Event()

    thread = Thread(target=infra_monitoring, args=(event, thread_id))
    thread.start()

    threads[thread_id] = {}
    jobs[thread_id] = {}
    threads[thread_id]["event"] = event
    threads[thread_id]["thread"] = thread
    threads[thread_id]["active"] = True
    jobs[thread_id]["data"] = ["infrastructure"]
    jobs[thread_id]["active"] = True
    thread_id = thread_id + 1



def infra_monitoring(event, thread_idx):
    global ec
    p = ec.createKafkaProducer()
    topic = "infrastructure"
    ec.deleteKafkaTopic(topic)
    time.sleep(1)
    ec.createKafkaTopic(topic)

    resources = pd.DataFrame(columns=["totCPU (mCore)", "totmem"])
    resources = pd.concat([resources, pd.DataFrame([[64000,388000]], columns=resources.columns)])

    #print(resources)


    header = True

    while(1):
        try:
            # Run the script and capture output
            result = subprocess.run(
                ["bash", "topnode.sh"],
                capture_output=True,
                text=True,
                check=True  # raises exception if script fails
            )

        except subprocess.CalledProcessError as e:
            print("Script failed:")
            print("Return code:", e.returncode)
            print("Error output:", e.stderr)


        x = result.stdout.split("\n")

        utilized = pd.DataFrame(columns=["node", "used_cpu (mCore)", "used_mem(Mi)"])
        utilizedCPU = 0
        utilizedMem = 0
        for i in range(1,len(x)-1):
            utilizedCPU = utilizedCPU + int(x[i].split()[1].strip("m"))
            utilizedMem = utilizedMem + int(x[i].split()[3].strip("Mi"))

        #resources["utilized CPU"] = utilizedCPU
        #resources["utilized Mem"] = utilizedMem
        #resources["percetage CPU"] = resources["utilized CPU"]/resources["totCPU (mCore)"]
        #resources["percetange Mem"] = resources["utilized Mem"]/resources["totmem"]
        #resources["available mCore"] =resources["totCPU (mCore)"] - resources["utilized CPU"]
        #resources["available Mem"] =resources["totmem"] - resources["utilized CPU"]
        resources["available mCore"] =resources["totCPU (mCore)"] - utilizedCPU
        resources["available Mem"] =resources["totmem"] - utilizedMem
        print(f"infrastructure={resources.to_string(index=False)}")
        
        #p.produce(topic, value=resources.to_string(index=False), callback=delivery_callback)
        p.produce(topic, value=resources.to_json(index=False), callback=delivery_callback)
        if event.is_set():
            break
        time.sleep(infra_polling)
        
    

def mon_function(event, thread_idx, service, namespace, pod):
    global num_client
    global ec
    p = ec.createKafkaProducer()
    topic = f"{service}_{namespace}_{pod}"
    ec.deleteKafkaTopic(topic)
    time.sleep(1)
    ec.createKafkaTopic(topic)

    t0 = time.time()
    if save_data:
        init_csv()
    print('Telemetry job {} starting...'.format(thread_idx))
    while True:
        raw = get_pod_cpu(namespace)
        #print(mess)
        #print(raw)
        mess = raw + f";{num_client};"
        if save_data:
            savedata(mess)
        p.produce(topic, value=mess, callback=delivery_callback)
        p.flush()
        p.poll(1)

        #print(mess)
        
        if deploy_client:
            if time.time() - t0 >= client_period and num_client < 11:
               t0 = time.time()
               run_client()

        if event.is_set():
            break
        time.sleep(period)
    print('Telemetry thread {} closing down'.format(thread_idx))




@monitoring.route('/startjob/<string:service>/<string:namespace>/<string:pod>')
@monitoring.response(200, 'Success')
@monitoring.response(404, 'Error, not found')
class _startTelemetry(Resource):
    @monitoring.doc(description="start telemetry")
    @staticmethod
    def put(service, namespace, pod):
        global threads
        global thread_id
        global jobs

        event = Event()

        thread = Thread(target=mon_function, args=(event, thread_id, service, namespace, pod))
        thread.start()

        threads[thread_id] = {}
        jobs[thread_id] = {}
        threads[thread_id]["event"] = event
        threads[thread_id]["thread"] = thread
        threads[thread_id]["active"] = True
        jobs[thread_id]["data"] = [service, namespace, pod]
        jobs[thread_id]["active"] = True
        thread_id = thread_id + 1
        return thread_id - 1, 200


@monitoring.route('/stopjob/<int:job_id>')
@monitoring.response(200, 'Success')
@monitoring.response(404, 'Error, not found')
class _stopTelemetryData(Resource):
    @monitoring.doc(description="stop telemetry")
    @staticmethod
    def delete(job_id):
        global threads
        global thread_id
        global jobs
        if threads[job_id]["active"]: 
            event = threads[job_id]["event"]
            thread = threads[job_id]["thread"]
            event.set()
            # wait for the new thread to finish
            thread.join()
            threads[job_id]["active"] = False
            jobs[job_id]["active"] = False
            return job_id , 200
        return thread_id , 404


@monitoring.route('/getjobs')
@monitoring.response(200, 'Success')
@monitoring.response(404, 'Error, not found')
class _getTelemetry(Resource):
    @staticmethod
    def get():
        global jobs
        return jobs, 200

#todo adapt num_client to a dict
@monitoring.route('/clients/<int:client>')
@monitoring.response(200, 'Success')
@monitoring.response(404, 'Error, not found')
class _getTelemetry(Resource):
    @staticmethod
    def put(client):
        global num_client
        num_client = client
        return 200



if __name__ == '__main__':
    infra_thread()
    app.run(host='0.0.0.0', port=portx)
