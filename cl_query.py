import pandas as pd
import time
from clickhouse_driver import Client
import json
import os



def run_query(query):
    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 500)
    host = 'clickhouse-eval.es.net'
    port = 9440
    f = open('/home/annagian/lala.json')
    data = json.load(f)
    f.close()

    user = data["data"]["username"]
    password = data["data"]["password"]
    print(user)
    print(password)
    try:
        cl_client = Client(host=host,database='ht',port=port,secure=True , verify=False , user=user , password=password)
        start_time = time.time()
        result , columns = cl_client.execute(query,with_column_types=True)
        df = pd.DataFrame(result,columns=[tuple[0] for tuple in columns])
        print ( "  CL Query time .. " , time.time() - start_time)
        print(df)
        return df
    except Exception as err:
        print("Exception could not open client",err)
        return err


def main():
    QUERY = """   SELECT 
      start_time_ns,
      max(end_time_ns) as max_end_time,
      toDecimal64(max_end_time,9) - toDecimal64(start_time_ns,9) as duration,
      any(hash_fwd) as h_fwd,
      any(exporting_node) as exp_node,
      anyIf(router_name,direction == 'in'  ) as in_router,
      anyIf(router_name,direction == 'out' ) as out_router,
      anyIf(router_interface,direction == 'in'  ) as in_router_interface,
      anyIf(router_interface,direction == 'out' ) as out_router_interface,
      anyIf(sap_routing_instance , direction == 'in' ) as in_sap,
      anyIf(sap_routing_instance , direction == 'out' ) as out_sap,
       any(ip_src) AS src,
       any(ip_dst) as dst,
       any(l4_src_port) as src_port,
       any(l4_dst_port) as dst_port,
      sumIf(tcp_loss_events , direction == 'in')  as loss_in,
      sumIf(tcp_loss_events , direction == 'out') as loss_out,
      sumIf(packets , direction == 'in') as pkts_in,
      sumIf(packets , direction == 'out') as pkts_out,
       sumIf(tcp_retrans_events , direction == 'in') as in_retran,
       sumIf(tcp_retrans_events , direction == 'out') as out_retran
    FROM ht.all_flows_comprehensive
    WHERE (end_time_ns > (now() - toIntervalMinute(5)))
    AND   packets > 2
    AND (router_name == 'fnalgcc-cr6' OR router_name=='fnalfcc-cr6')
    GROUP BY start_time_ns
    ORDER BY start_time_ns ASC  FORMAT CSV

    """
    while True:
        df = run_query(QUERY)
        print('GCC router flows in the last 5 min...', (df['in_router']=='fnalgcc-cr6').sum())
        print('FCC router flows in the last 5 min...', (df['in_router']=='fnalfcc-cr6').sum())
        compression_opts = dict(method='zip',archive_name='fnal_test5.csv')
        df.to_csv('fnal_test5min.zip', index=False,compression=compression_opts)
        time.sleep(300)
    


if __name__=="__main__":
	main()

