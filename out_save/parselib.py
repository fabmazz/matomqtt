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