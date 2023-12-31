import numpy as np
import polars as pl

def haversine_distance(lat1, lon1, lat2, lon2):
    # Convert latitude and longitude from degrees to radians
    lat1 = np.radians(lat1)
    lon1 = np.radians(lon1)
    lat2 = np.radians(lat2)
    lon2 = np.radians(lon2)

    # Haversine formula
    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))

    # Radius of the Earth in kilometers (mean value)
    earth_radius_km = 6371.0

    # Calculate the distance
    distance = earth_radius_km * c *1000

    return distance

def filter_trip_df(df_trip):
    df = df_trip.sort("timerec")

    delts = df["timerec"][1:]-df["timerec"][:-1]

    delta_sec = delts.dt.milliseconds()/1000

    filt = (delta_sec==0)

    idcs_jump = []

    if np.any(filt.to_numpy()):
        idcs=np.where(filt)[0]
        print(f"{len(idcs)} to filter out")
        
        for i in idcs:
            i  = int(i)
            for col in ["lat","lon","heading"]:
            
                u=df[i:i+2][col].mean()
                df[i+1,col] = u
                ## mark row for removal
                df[i,"direct"]=-20
            idcs_jump.append(i)

    df = df.filter(pl.col("direct")!=-20)

    return df

def find_speed_trip(df_trip, first_idx=-20, window=4):
    df = df_trip
    dist = haversine_distance(df["lat"][:-1], df["lon"][:-1], df["lat"][1:], df["lon"][1:]).rename("dist")

    ## speed
    delts = df["timerec"][1:]-df["timerec"][:-1]
    time_pd = df["timerec"].to_pandas()
    deltpd = delts.to_pandas()
    time_speed = deltpd/2 + time_pd[:-1]
    sec_delta = (delts.dt.milliseconds()/1000)
    speed_kmh = dist/sec_delta * 3.6
    speed_sm =  speed_smooth(dist, sec_delta, window)

    times = (np.where(dist > 50)[0]).tolist()
    ## index it starts to move
    """if(first_idx < 0):
        firstindex=0
        print(times[:30])
        for x in times:
            if x+1 in times:

                firstindex=x
                print("Found first index: ",firstindex)
                break
                break
    else: 
        firstindex = first_idx"""
    if(first_idx<0):
        firstindex = find_firstindex_move(speed_kmh)
    else: firstindex = first_idx

    
    trace_df= pl.DataFrame([dist[firstindex:], pl.from_pandas(time_speed[firstindex:]).rename("time"), speed_kmh[firstindex:].rename("speed"),
                            pl.Series("speed_smooth",speed_sm[firstindex:]),
                            sec_delta[firstindex:].rename("delta_secs"),
                            ])

    return trace_df, firstindex
"""
New function to calculate the speed (computes time and space differences)
"""
def calc_speed_trip(dft):
    tdiff = dft["timerec"].diff(null_behavior="drop")
    delta_s = tdiff.dt.seconds().alias("diff_s")
    assert np.all(delta_s.to_numpy()>0)

    diff_metr = dist_trace_df(dft).alias("diff_m")

    lat_avg = (dft[:-1,"lat"]+dft[1:, "lat"])/2
    lon_avg = (dft[:-1,"lon"]+dft[1:, "lon"])/2
    tr=dft["timerec"].to_pandas()

    return lat_avg.hstack(lon_avg).hstack([diff_metr, delta_s,
       pl.Series((tr+tdiff.to_pandas()/2)[:-1]).alias("t_mean")]).with_columns(
        (pl.col("diff_m")*3.6/pl.col("diff_s")).alias("speed_kms"))

def smooth_window(signal, w):
    T=len(signal)
    res = np.zeros(T)
    for t in range(0, T):
        #if sum(infect[max(0, t-w):t+1]) > 0:
        #    Reff[t] = (sum(newinf[max(0,t-w):t+1]) / sum(infect[max(0,t-w):t+1])) * mean_inf
        lt=max(0,t-w)
        res[t] = sum(signal[lt:t+1]) /(t+1-lt)
    return res

def find_firstindex_move(speed_kmh):
    nidx = len(speed_kmh)
    maxidx = int(0.4*nidx)
    sp_sm = smooth_window(speed_kmh[:maxidx],3)
    idcs=np.where(sp_sm < 0.8)[0]
    z=np.zeros_like(sp_sm,dtype=int)
    for id in idcs:
        z[id] = z[id-1]+1
    return np.argmax(z)

def speed_smooth(dist, times_s, w):
    T=len(dist)
    res = np.zeros(T)
    for t in range(0, T):
        lt=max(0,t-w)
        res[t] = sum(dist[lt:t+1]) /sum(times_s[lt:t+1]) *3.6
    
    return res

fast_dist = lambda geom: haversine_distance(geom[:-1,0],geom[:-1,1], geom[1:,0], geom[1:,1] )

def dist_trace_df(df):
    return haversine_distance(df["lat"][:-1], df["lon"][:-1], df["lat"][1:], df["lon"][1:]).rename("distance")

def add_midpoints_trace(geom, dist, max_dist, idc_point, verbose=False):
    assert len(geom) == len(dist)+1

    geom2 = geom ## shallow copy, it's overwritten later

    npoints = int(dist[idc_point] / max_dist)
    npoints, idc_point

    stepx = (geom[idc_point+1,0]-geom[idc_point,0])/(npoints+1)
    stepy = (geom[idc_point+1,1]-geom[idc_point,1])/(npoints+1)
    #newps=[]
    steps = np.array([stepx, stepy])
    if verbose: print("steps: ",steps)

    for i in range(npoints):
        newp = geom[idc_point]+steps*(i+1)
        #[geom[idc_dist,0]+stepx*(i+1), geom[idc_dist,1]+stepy*(i+1)]
        if verbose: print(newp)
        geom2 = np.insert(geom2, idc_point+i+1, newp, axis=0)

    return geom2, npoints

"""
Put the midpoints inside a trace (list of (lat, lon) coordinates) for each segment with a distance lower than max
"""
def put_midpoints_trace_all(geom, max_dist,v=False):
    added_pts = 0
    geom2 = geom
    dist_u = fast_dist(geom2) #mlib.haversine_distance(geom2[:-1,0],geom2[:-1,1], geom2[1:,0], geom2[1:,1] )
    sv_dist = (sum(dist_u))
    nt= 0
    while (dist_u.max()> max_dist):
        nt+=1
        if nt > 2000:
            print("BREAK")
            break
        if v: print(added_pts)

        idc_dist = np.where(dist_u > max_dist)[0] [0]
        #print("idc:",np.where(dist_u > max_dist)[0])

        geom2,npp = add_midpoints_trace(geom2, dist_u, max_dist,idc_dist)

        added_pts += npp
        dist_u = fast_dist(geom2)
    
    print(sum(dist_u)-sv_dist)
    return geom2, dist_u

def perf_average_if_poss(df, max_dist=130):
    coord =  (df["lat"].mean(), df["lon"].mean())
    dist_all = haversine_distance(df["lat"], df["lon"], *coord).to_numpy()
    #dists = (mlib.haversine_distance(df["lat"].max(), coord[1], df["lat"].min(), coord[1]),
    #         mlib.haversine_distance(coord[0],df["lon"].max(), coord[0], df["lon"].min()))
    if np.min(dist_all) > max_dist:
        return None
    
    draw=df[0:1].clone()
    draw[0,"lat"]=coord[0]
    draw[0,"lon"]= coord[1]
    draw[0,"heading"] = (df["heading"].mean())
    draw[0,"speed"]=0.
    
    return draw #pl.from_dict(draw)

def average_times_if_needed(df_trip):
    dfin = df_trip.sort("timerec")
    out=[]
    n_tot = 0
    n_ref = 0
    if not (dfin.group_by("timerec").count()["count"]>1).any():
        #print(f"key {k} nothing to do")
        #print(dfin.group_by("timerec").count())
        return dfin
    
    for tt, df in dfin.group_by("timerec"):
        #print(tt)
        n_tot +=1
        if len(df) > 1:
            re=perf_average_if_poss(df)
            if re is not None:
                out.append(re)
            else:
                n_ref +=1
        else:
            out.append(df)
    if len(out) == 0:
        #ltime=(dfin["timerec"][-1]-dfin["timerec"][0]).total_seconds()
        #if dfin
        return None
    dfnew = pl.concat(out).sort("timerec")
    return dfnew