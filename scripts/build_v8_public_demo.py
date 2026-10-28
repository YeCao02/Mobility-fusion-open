from __future__ import annotations
import os
os.environ["POLARS_MAX_THREADS"]="2";os.environ["OMP_NUM_THREADS"]="1";os.environ["PYTHONDONTWRITEBYTECODE"]="1"
import json,gzip,hashlib,hmac,secrets,importlib.util,time,multiprocessing as mp
from pathlib import Path
import polars as pl
WORK=Path("J:/Mobility-Computing/phone_spatiotemporal/work/shanghai_closeout_20261028/open_demo");WORK.mkdir(parents=True,exist_ok=True)
REPO=Path(__file__).resolve().parents[1]
ROOT=Path("J:/Mobility-Computing/mobility_upgrade_20261018")
PACKAGES={"gba":ROOT/"gba_full_v8_20261024","sh":ROOT/"shanghai_full_v8_20261026/residence"}
CITIES={"440100000000":"gz","440300000000":"sz","440400000000":"zh","440600000000":"fs","440700000000":"jm","441200000000":"zq","441300000000":"hz","441900000000":"dg","442000000000":"zs"}
LOOKUP=Path("S:/GEO BIG data/Greater Bay Area data_operators 500G/mobility_fusion_production_v0_4_0/ref_inputs/gba_h3_lookup_res10.parquet")
R=None;CAT=None;PKG=None

def init(cohort):
 global R,CAT,PKG
 root=PACKAGES[cohort];current=json.loads((root/"CURRENT.json").read_text());PKG=root/current["relative_path"]
 assert hashlib.sha256((PKG/"COMPLETE.json").read_bytes()).hexdigest()==current["complete_sha256"]
 spec=importlib.util.spec_from_file_location("release_reader_"+cohort,PKG/"reader.py");R=importlib.util.module_from_spec(spec);spec.loader.exec_module(R);_,CAT=R.open_package(PKG)

def part_job(args):
 cohort,part_id=args;folder=WORK/"decoded_private"/cohort;folder.mkdir(parents=True,exist_ok=True);n=folder/(part_id+"_nodes.parquet");s=folder/(part_id+"_segments.parquet")
 if not (n.exists() and s.exists()):
  f=R._read_partition(PKG,CAT,part_id)
  f["nodes"].write_parquet(n,compression="zstd");f["segments"].write_parquet(s,compression="zstd")
 return part_id,str(n),str(s)

def select(cohort):
 target=WORK/(cohort+"_selection_PRIVATE.parquet")
 if target.exists():return pl.read_parquet(target)
 init(cohort);parts=CAT["partitions"];lookup=pl.read_parquet(LOOKUP,columns=["grid_id","city_code"]).with_columns(pl.col("city_code").cast(pl.String).replace_strict(CITIES,default=None).alias("city")).select(pl.col("grid_id").alias("h3_id"),"city") if cohort=="gba" else None
 desired=list(CITIES.values()) if cohort=="gba" else ["sh"];pools={c:[] for c in desired};seen=set();scanned=0
 with mp.get_context("spawn").Pool(4,initializer=init,initargs=(cohort,)) as pool:
  for offset in range(0,len(parts),4):
   jobs=[(cohort,p["id"]) for p in parts[offset:offset+4]]
   for part_id,np,sp in pool.map(part_job,jobs):
    n=pl.read_parquet(np,columns=["uuid","date","node_order","node_role","h3_id"]).sort(["uuid","date","node_order"])
    first=n.filter(pl.col("node_role")=="DAY_START").unique(subset=["uuid"],keep="first",maintain_order=True)
    if cohort=="gba":first=first.join(lookup,on="h3_id",how="inner").filter(pl.col("city").is_not_null())
    else:first=first.with_columns(pl.lit("sh").alias("city"))
    for uid,city in first.select("uuid","city").iter_rows():
     if uid in seen:continue
     seen.add(uid)
     if city in pools and len(pools[city])<2400:pools[city].append(dict(uuid=uid,city=city,partition=part_id,cohort=cohort))
    scanned+=1
   print(json.dumps(dict(stage="candidates",cohort=cohort,partitions=scanned,counts={c:len(v) for c,v in pools.items()})),flush=True)
   if all(len(v)>=2000 for v in pools.values()):break
 assert all(len(v)>=2000 for v in pools.values())
 saltfile=WORK/"PUBLIC_ID_SALT_PRIVATE.bin"
 if not saltfile.exists():saltfile.write_bytes(secrets.token_bytes(32))
 salt=saltfile.read_bytes();chosen=[]
 for city,values in pools.items():
  values.sort(key=lambda d:hmac.new(salt,(cohort+"|"+d["uuid"]).encode(),hashlib.sha256).digest())
  for row in values[:2000]:row["public_id"]=city+"-"+hmac.new(salt,(cohort+"|"+row["uuid"]).encode(),hashlib.sha256).hexdigest()[:20];chosen.append(row)
 f=pl.DataFrame(chosen);assert f["public_id"].n_unique()==len(chosen);f.write_parquet(target,compression="zstd");return f


def selected_frames(cohort,selection):
 folder=WORK/"selected_private"/cohort;folder.mkdir(parents=True,exist_ok=True);np=folder/"nodes.parquet";sp=folder/"segments.parquet"
 if np.exists() and sp.exists():return np,sp
 ns=[];ss=[]
 for part,g in selection.group_by("partition"):
  part=part[0];uids=g["uuid"];base=WORK/"decoded_private"/cohort
  ns.append(pl.read_parquet(base/(part+"_nodes.parquet")).filter(pl.col("uuid").is_in(uids.implode())))
  ss.append(pl.read_parquet(base/(part+"_segments.parquet")).filter(pl.col("uuid").is_in(uids.implode())))
 pl.concat(ns,how="diagonal_relaxed").write_parquet(np,compression="zstd");pl.concat(ss,how="diagonal_relaxed").write_parquet(sp,compression="zstd");return np,sp

def raw_sources(cohort):
 request=Path("G:/Mobile_Data_Standardized/gba_cache_reuse_20261024/FULL_REQUEST_PRIVATE.json") if cohort=="gba" else ROOT/"shanghai_trajectory_20261026/residence/PLAN_R12_PRIVATE.json"
 return json.loads(request.read_text())["datasets"][0]["sources"]

def migrate_source_path(cohort,path):
 p=Path(path)
 if cohort=="gba":return Path("G:/Mobile_Data_Standardized/gba_cache_reuse_20261024")/str(p).replace("\\","/").split("gba_cache_reuse_20261024/",1)[1]
 if p.exists():return p
 mapping=json.loads((WORK.parent/"storage/RECYCLED_INPUT_ROOT_MAPPING_PRIVATE.json").read_text())
 for entry in mapping:
  old=Path(entry["original_root"])
  if p.is_relative_to(old):return Path(entry["recycled_readonly_root"])/p.relative_to(old)
 raise FileNotFoundError("No read-only accepted daily-view migration mapping")

def extract_raw(cohort,selection):
 import pyarrow.parquet as pq,bisect
 folder=WORK/"raw_private"/cohort;folder.mkdir(parents=True,exist_ok=True);done=folder/"DONE_PUBLIC.json"
 if done.exists():return folder
 uids=sorted(selection["uuid"].to_list());wanted=pl.Series("uuid",uids);reads=0;rows=0
 columns=["uuid","date","point_source","lon","lat","start_time","end_time","negative_duration_flag","raw_strategy_status","h3_10"]
 bindings=[]
 for i,source in enumerate(raw_sources(cohort)):
  p=migrate_source_path(cohort,source["path"]);pf=pq.ParquetFile(p);idx=pf.schema_arrow.names.index("uuid");matches=False
  for j in range(pf.num_row_groups):
   st=pf.metadata.row_group(j).column(idx).statistics
   if st is None or not st.has_min_max:matches=True;break
   k=bisect.bisect_left(uids,str(st.min))
   if k<len(uids) and uids[k]<=str(st.max):matches=True;break
  if not matches:continue
  before=p.stat();assert source.get("bytes",before.st_size)==before.st_size and source.get("mtime_ns",before.st_mtime_ns)==before.st_mtime_ns;bindings.append(dict(date=source["date"],source_sha256=source["sha256"],bytes=before.st_size,mtime_ns=before.st_mtime_ns));lf=pl.scan_parquet(p,hive_partitioning=False);schema=lf.collect_schema();f=lf.filter(pl.col("uuid").is_in(wanted.implode())).select([c for c in columns if c in schema]).collect(engine="streaming");assert p.stat().st_size==before.st_size and p.stat().st_mtime_ns==before.st_mtime_ns
  if f.height:
   if "h3_10" not in f.columns:
    import h3
    f=f.with_columns(pl.Series("h3_10",[h3.latlng_to_cell(lat,lon,10) for lon,lat in f.select("lon","lat").iter_rows()]))
   if "negative_duration_flag" not in f.columns:f=f.with_columns((pl.col("end_time")<pl.col("start_time")).alias("negative_duration_flag"))
   if "raw_strategy_status" not in f.columns:f=f.with_columns(pl.lit("STANDARDIZED_ACCEPTED").alias("raw_strategy_status"))
   f.write_parquet(folder/(str(i).zfill(4)+".parquet"),compression="zstd");rows+=f.height
  reads+=1;print(json.dumps(dict(stage="raw_extract",cohort=cohort,source_index=i,files_read=reads,selected_rows=rows)),flush=True)
 done.write_text(json.dumps(dict(cohort=cohort,files_read=reads,rows=rows,source_bindings=bindings,binding_scope="Selected standardized source bytes/mtime stable; SHA references accepted production or migration receipts, no duplicate full input rehash")));return folder

def adapt_nodes(n):
 mask=pl.lit(0)
 for col,bit in [("timing_event_count",1),("wifistable_event_count",2),("wificonnect_event_count",4),("scenereco_event_count",8)]:mask=mask+pl.when(pl.col(col)>0).then(bit).otherwise(0)
 return n.rename({"h3_id":"h3_10","h3_9_id":"h3_9","start_time":"start_time_ms","end_time":"end_time_ms"}).with_columns(pl.col("node_role").replace_strict({"DAY_START":1,"STAY":2,"STOP":3,"DAY_END":4},default=3).alias("node_role_code"),mask.alias("source_mask"),pl.lit(0).alias("station_evidence_code"),pl.lit("").alias("scenereco_p_names"))

def adapt_segments(s,n):
 endpoints=n.select("uuid","date","node_order","h3_10")
 o=endpoints.rename({"node_order":"o_node_order","h3_10":"o_h3_10"});d=endpoints.rename({"node_order":"d_node_order","h3_10":"d_h3_10"})
 out=s.join(o,on=["uuid","date","o_node_order"],how="left",validate="m:1").join(d,on=["uuid","date","d_node_order"],how="left",validate="m:1");assert out.height==s.height and out["o_h3_10"].null_count()==0 and out["d_h3_10"].null_count()==0
 return out.rename({"depart_time":"depart_time_ms","arrive_time":"arrive_time_ms","od_invalid_reason":"od_invalid_reason_code"})

def export(cohort):
 import build_open_demo_data as old
 old.CITY_NAME_BY_ABBR["sh"]="Shanghai / 上海";old.SOURCE_CODE["WifiConnect"]="C"
 lookup=pl.read_parquet(LOOKUP,columns=["grid_id","city_code","city_name"]).with_columns(pl.col("city_code").cast(pl.String).replace_strict(CITIES,default=None).alias("abbr"));lookupdict={r["grid_id"]:dict(code=r["city_code"],abbr=r["abbr"] or "",name=r["city_name"]) for r in lookup.iter_rows(named=True)}
 old.h3_city_lookup=lambda cells,default_city="":{c:lookupdict.get(c,dict(code=None,abbr=None,name=None)) for c in cells}
 selection=select(cohort);assert selection["uuid"].n_unique()==selection.height;assert lookup["grid_id"].n_unique()==lookup.height;np,sp=selected_frames(cohort,selection);rawfolder=extract_raw(cohort,selection)
 n=adapt_nodes(pl.read_parquet(np));s=adapt_segments(pl.read_parquet(sp),n);rawscan=pl.scan_parquet(str(rawfolder/"*.parquet"));dates=["2024-12-24","2024-12-29"] if cohort=="gba" else ["2026-05-"+str(i).zfill(2) for i in range(1,15)];publicroot=REPO/("demo/data/v8" if cohort=="gba" else "demo/data/v8_sh");publicroot.mkdir(parents=True,exist_ok=True)
 manifest=dict(generated_at=time.strftime("%Y-%m-%dT%H:%M:%S+08:00"),description="Frozen-v8 complete-user sample from accepted full production. Fixed 2000 pseudonymous users per city; all available delivered dates retained; not a population-representative sample.",h3_res=10,scientific_version="frozen_v8",cohort=("gba_2024" if cohort=="gba" else "shanghai_residence_2026"),city_assignment=("earliest available DAY_START H3 city; not residence city" if cohort=="gba" else "Shanghai residence cohort"),h3_city_lookup_sha256=hashlib.sha256(LOOKUP.read_bytes()).hexdigest(),users_per_city=2000,dates=dates,privacy="Fresh keyed pseudonyms; private identity map and salt are not published. Original locations/time remain visible for method demonstration, not anonymous population statistics.",cities={})
 counts=[];all_public_ids=set(selection["public_id"]);raw_count=0;node_count=0;od_count=0
 for (city,),cityselection in selection.group_by("city",maintain_order=True):
  cityselection=cityselection.sort("public_id");citymeta=dict(name=old.CITY_NAME_BY_ABBR[city],unique_users=cityselection.height,files=[]);manifest["cities"][city]=citymeta
  for date in dates:
   for page,start in enumerate(range(0,cityselection.height,200),1):
    chunk=cityselection.slice(start,200);uids=chunk["uuid"];mapping=dict(zip(chunk["uuid"],chunk["public_id"]));nn=n.filter((pl.col("date")==date)&pl.col("uuid").is_in(uids.implode()));ss=s.filter((pl.col("date")==date)&pl.col("uuid").is_in(uids.implode()));ee=rawscan.filter((pl.col("date").cast(pl.String)==date)&pl.col("uuid").is_in(uids.implode())).collect(engine="streaming")
    assert ss.height==0 or nn.height>0
    nn=nn.with_columns(pl.col("uuid").replace_strict(mapping).alias("uuid"));ss=ss.with_columns(pl.col("uuid").replace_strict(mapping).alias("uuid"));ee=ee.with_columns(pl.col("uuid").replace_strict(mapping).alias("uuid"));assert set(ee["uuid"].unique()).issubset(all_public_ids)
    processed=old.build_processed_payload(city,date,nn,ss);raw=old.build_raw_payload(date,ee,nn)
    originals={(row["uuid"]+"__"+date,row["node_order"]):row for row in nn.iter_rows(named=True)}
    segment_originals={(row["uuid"]+"__"+date,row["segment_order"]):row for row in ss.iter_rows(named=True)}
    for case,content in processed["uuid_data"].items():
     for item in content["nodes"]:
      row=originals[(case,item["o"])];item.update(h10c=row.get("h3_10_child_count"),mh3=row.get("merged_h3_ids"),mc=row.get("merged_node_count"),se=None,dur=row.get("duration_sec"),pn="",city=(lookupdict.get(item["h3"],{}).get("abbr") or lookupdict.get(item["h3"],{}).get("name")))
     for item in content["od"]:
      row=segment_originals[(case,item["so"])];item.update(bd=row.get("boundary_unresolved"),drop=None,qc=row.get("quality_flags"),tt=row.get("travel_time_sec"),cross=(lookupdict[item["oh3"]]["code"]!=lookupdict[item["dh3"]]["code"] if item["oh3"] in lookupdict and item["dh3"] in lookupdict else None))
    empty_summary=dict(total=0,kept=0,dropped=0,anchor_suppressed=0,sources={})
    for public in chunk["public_id"]:
     case=public+"__"+date
     if case not in raw["uuid_data"]:raw["uuid_data"][case]=dict(events=[],event_summary=dict(empty_summary))
     if case not in processed["uuid_data"]:
      processed["uuid_data"][case]=dict(nodes=[],od=[],drop_nodes=[],event_summary={},no_production_nodes=True)
      processed["uuid_summary"].append(dict(uuid=case,short=public+" "+date[-5:],nodes=0,od=0,st=0,sp=0,cross=0,hs=0,bf=0,pr=0,score=0))
     processed["uuid_data"][case]["event_summary"].update(raw["uuid_data"][case]["event_summary"])
     processed["uuid_data"][case]["no_observations"]=not bool(raw["uuid_data"][case]["events"] or processed["uuid_data"][case]["nodes"])
    processed["stats"]["uuid_count"]=chunk.height
    assert set(processed["uuid_data"])==set(raw["uuid_data"]) and len(processed["uuid_data"])==chunk.height
    for q in processed["uuid_summary"]:
     q["short"]=q["uuid"].split("__")[0]+" "+date[-5:];q["cross"]=sum(x["cross"] is True for x in processed["uuid_data"][q["uuid"]]["od"]);q["cross_unknown"]=sum(x["cross"] is None for x in processed["uuid_data"][q["uuid"]]["od"])
    names={};sizes={}
    for kind,payload in [("processed",processed),("raw",raw)]:
     name=f"{kind}_{city}_{date}_p{page:02d}.json.gz";content=json.dumps(payload,ensure_ascii=False,separators=(",",":"),allow_nan=False).encode();p=publicroot/name;p.write_bytes(gzip.compress(content,compresslevel=9,mtime=0));names[kind]=("data/v8/" if cohort=="gba" else "../data/v8_sh/")+name;sizes[kind]=p.stat().st_size
    citymeta["files"].append(dict(date=date,page=page,**names,uuid_count=len(processed["uuid_data"]),raw_event_count=ee.height,node_count=nn.height,od_count=ss.height,compressed_bytes=sizes))
    raw_count+=ee.height;node_count+=nn.height;od_count+=ss.height;print(json.dumps(dict(stage="export",cohort=cohort,city=city,date=date,page=page,users=len(processed["uuid_data"]),gzip_bytes=sum(sizes.values()))),flush=True)
  counts.append(dict(city=city,unique_users=cityselection.height,cases=sum(x["uuid_count"] for x in citymeta["files"])))
 assert raw_count==json.loads((rawfolder/"DONE_PUBLIC.json").read_text())["rows"] and node_count==n.height and od_count==s.height
 (REPO/("demo/data/manifest_v8.json" if cohort=="gba" else "demo/shanghai-validation/data/manifest_v8.json")).write_text(json.dumps(manifest,ensure_ascii=False,separators=(",",":")))
 receipt=dict(cohort=cohort,city_counts=counts,unique_users=selection["uuid"].n_unique(),public_ids_unique=selection["public_id"].n_unique(),raw_original_identity_unique=selection["uuid"].n_unique(),raw_rows=raw_count,nodes=node_count,segments=od_count,files=len(list(publicroot.glob("*.gz"))),gzip_bytes=sum(p.stat().st_size for p in publicroot.glob("*.gz")),max_file_bytes=max(p.stat().st_size for p in publicroot.glob("*.gz")),dates=dates,pseudonym_only=True,no_original_uuid_published=True,raw_processed_keys_exact=True,complete_available_user_context=True)
 (WORK/(cohort+"_EXPORT_PUBLIC.json")).write_text(json.dumps(receipt,indent=2));print(json.dumps(receipt),flush=True)

if __name__=="__main__":
 import argparse
 a=argparse.ArgumentParser();a.add_argument("--select",choices=["gba","sh","both"],default="both");a.add_argument("--export",action="store_true");args=a.parse_args()
 for c in (["gba","sh"] if args.select=="both" else [args.select]):
  f=select(c);print(json.dumps(dict(stage="selection_done",cohort=c,counts=f.group_by("city").len().to_dicts())),flush=True)
  if args.export:export(c)
