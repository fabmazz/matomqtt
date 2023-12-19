import numpy as np
import numba as nb
from parselib import haversine_distance

hvs_nb = nb.njit(haversine_distance)


def row_avg(df, j, i, count):
    for k in ["lat", "lon", "heading"]:
        if k=="heading":
            if df[j,k] == None:
                df[j,k] = df[i,k] ## ignore None heading
        else:
            df[j,k] = (df[j,k]*count[j] + df[i,k])/(count[j]+1)

    count[j]+=1
    count[i]=0

def average_pos_equaltime(spec_df):
    count = np.ones(len(spec_df),dtype=np.int_)
    secs_diff = spec_df["timerec"].diff().dt.seconds()

    idcs_eq = np.where(secs_diff == 0)[0]
    for i in idcs_eq:
        for j in range(i-1,-1,-1):
            if count[j] > 0:
                row_avg(spec_df, j, int(i), count)
                break
    
    return spec_df.filter(count>0)

@nb.njit()
def find_closest_trace_second(trace_np, geom_np):
    N = len(trace_np)
    M  = len(geom_np)
    dist_min = np.full(N,1e5)
    dist_sec = np.full(N,1e7)
    idc_min = np.full(N,-10)
    idc_sec = np.full(N,-10)
    for j in range(M):
        dd = hvs_nb(trace_np[:,0], trace_np[:,1], geom_np[j,0], geom_np[j,1])
        m = dd < dist_min
        ## second mask
        m2 = (~m) & (dd<dist_sec)
        ## copy data on dist
        dist_sec[m] = dist_min[m]
        idc_sec[m] = idc_min[m]
        dist_min[m] = dd[m]
        idc_min[m] = j
        ## second part
        dist_sec[m2] = dd[m2]
        idc_sec[m2] = j
    return idc_min, idc_sec, dist_min, dist_sec


@nb.njit()
def find_closest_tr(trace_np, geom_np):
    N = len(trace_np)
    M  = len(geom_np)
    dist_min = np.full(N,1e5)
    idc_min = np.full(N,-10)
    for j in range(M):
        dd = hvs_nb(trace_np[:,0], trace_np[:,1], geom_np[j,0], geom_np[j,1])
        m = dd < dist_min
        dist_min[m] = dd[m]
        idc_min[m] = j

    return idc_min, dist_min