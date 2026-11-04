#!/bin/sh
# Node probe: run in a privileged hostPID pod. Samples /proc for DURATION seconds and prints a summary.
# Usage: probe.sh DURATION
D=${1:-150}
PAT='cloud-hyperv|virtiofsd|ateom|ateworker|runsc|^exe$|atelet|objectstore|containerd-shim|kata|gofer'
T=/tmp/probe; mkdir -p $T; cd $T || exit 1

list_pids() {
  for p in /proc/[0-9]*; do
    c=$(cat $p/comm 2>/dev/null) || continue
    echo "$c" | grep -Eq "$PAT" && echo "${p#/proc/} $c"
  done
}
snap_cpu() {  # all processes: pid comm utime+stime   (comm may contain spaces: strip "pid (comm) ")
  for p in /proc/[0-9]*; do
    s=$(cat $p/stat 2>/dev/null) || continue
    c=$(cat $p/comm 2>/dev/null | tr ' ' '_')
    echo "${p#/proc/} ${c:-?} $(echo "$s" | sed 's/^.*) //' | awk '{print $12+$13}')"
  done
}
snap_threads() {  # target threads: pid tid procname threadname utime+stime
  while read pid comm; do
    for t in /proc/$pid/task/[0-9]*; do
      s=$(cat $t/stat 2>/dev/null) || continue
      echo "$pid ${t##*/} $comm $(cat $t/comm 2>/dev/null | tr ' ' '_') $(echo "$s" | sed 's/^.*) //' | awk '{print $12+$13}')"
    done
  done < pids
}
snap_io() {
  while read pid comm; do
    awk -v c="$comm" '{v[$1]=$2} END{print c, v["read_bytes:"]+0, v["write_bytes:"]+0, v["syscw:"]+0, v["cancelled_write_bytes:"]+0}' /proc/$pid/io 2>/dev/null
  done < pids
}

list_pids > pids
echo "== probe start $(date -u +%T) duration ${D}s; target processes: $(wc -l < pids)"
awk '{print $2}' pids | sort | uniq -c | sort -rn | head -20
echo "== mount table size seen by ateom/atelet (lines in /proc/<pid>/mountinfo)"
grep -E ' (ateom|ateworker|atelet)' pids | while read pid comm; do echo "$comm pid=$pid mounts=$(wc -l < /proc/$pid/mountinfo 2>/dev/null)"; done
echo "host init mounts=$(wc -l < /proc/1/mountinfo 2>/dev/null)"
snap_cpu > cpu0; snap_threads > thr0; snap_io > io0
grep -E 'nr_dirty |nr_writeback |nr_dirtied|nr_written|pgpgout|pgpgin' /proc/vmstat > vm0
cat /proc/diskstats > disk0
: > samples; : > stacks; : > paths
# Per-actor CPU throttling: gVisor sandbox cgroups are <uid>-_sandbox. Every 5 s, sum
# cpu.stat deltas over cgroups present in both samples (actors come and go).
thr_snap() {
  for d in $(find /proc/1/root/sys/fs/cgroup -maxdepth 9 -type d -name '*-_sandbox' 2>/dev/null); do
    awk -v n="${d##*/}" -v m="$(cat $d/cpu.max 2>/dev/null | cut -d' ' -f1)" '{v[$1]=$2} END{print n, v["nr_periods"]+0, v["nr_throttled"]+0, v["throttled_usec"]+0, v["usage_usec"]+0, m}' $d/cpu.stat 2>/dev/null
  done
}
( thr_snap > thr_prev; : > thr_deltas; tend=$(( $(date +%s) + D ))
  while [ $(date +%s) -lt $tend ]; do sleep 5; thr_snap > thr_cur
    awk 'NR==FNR{p[$1]=$2; t[$1]=$3; u[$1]=$4; g[$1]=$5; next} ($1 in p){print $2-p[$1], $3-t[$1], $4-u[$1], $5-g[$1], $6}' thr_prev thr_cur >> thr_deltas
    wc -l < thr_cur >> thr_counts; awk '{print $6}' thr_cur | sort | uniq -c | tr '\n' ' ' >> thr_max; echo >> thr_max
    mv thr_cur thr_prev; done ) &
THR_PID=$!
end=$(( $(date +%s) + D ))
n=0
while [ $(date +%s) -lt $end ]; do
  while read pid comm; do
    for t in /proc/$pid/task/[0-9]*; do
      st=$(sed 's/^.*) //' $t/stat 2>/dev/null | cut -d' ' -f1) || continue
      [ "$st" = "R" ] || [ "$st" = "D" ] || continue   # only running / uninterruptible (the interesting ones)
      w=$(cat $t/wchan 2>/dev/null); sc=$(cut -d' ' -f1 $t/syscall 2>/dev/null)
      echo "$comm $st ${w:-0} ${sc:-?}" >> samples
      if [ "$st" = "D" ]; then
        set -- $(cat $t/syscall 2>/dev/null)
        case "$1" in 263|257|258) addr=$3;; 166) addr=$2;; *) addr="";; esac
        if [ -n "$addr" ]; then
          a=$(printf '%d' "$addr" 2>/dev/null) || a=0
          if [ "$a" -gt 4096 ]; then
            pth=$(dd if=/proc/$pid/mem bs=4096 skip=$((a/4096)) count=2 2>/dev/null | tail -c +$((a%4096+1)) | tr '\0' '\n' | head -1)
            echo "$1 $(echo "$pth" | sed -E 's/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/<uid>/g; s/[0-9a-f]{16,}/<hex>/g')" >> paths
          fi
        fi
      fi
      if [ "$st" = "D" ]; then stk=$(awk '{print $2}' $t/stack 2>/dev/null | sed 's/+0x.*//' | head -8 | paste -sd'<'); echo "$comm ${sc:-?} ${stk:-nostack}" >> stacks; fi
    done
  done < pids
  n=$((n+1)); sleep 1
done
snap_cpu > cpu1; snap_threads > thr1; snap_io > io1
grep -E 'nr_dirty |nr_writeback |nr_dirtied|nr_written|pgpgout|pgpgin' /proc/vmstat > vm1
cat /proc/diskstats > disk1
el=$D

echo "== samples: $n rounds"
echo "== top processes by CPU (cores avg over window)"
awk 'NR==FNR{a[$1]=$3; next} ($1 in a){d=$3-a[$1]; if(d>0) print d/100/'$el', $2}' cpu0 cpu1 | sort -rn | head -15
echo "== CPU by process name (cores)"
awk 'NR==FNR{a[$1]=$3; n[$1]=$2; next} ($1 in a){d=$3-a[$1]; if(d>0) s[$2]+=d} END{for(k in s) print s[k]/100/'$el', k}' cpu0 cpu1 | sort -rn | head -15
echo "== hottest threads (cores) [proc thread-name]"
awk 'NR==FNR{a[$2]=$5; next} ($2 in a){d=$5-a[$2]; if(d>0) print d/100/'$el', $3, $4}' thr0 thr1 | sort -rn | head -20
echo "== thread-name totals for target procs (cores, threads)"
awk 'NR==FNR{a[$2]=$5; next} ($2 in a){d=$5-a[$2]; k=$3" "$4; s[k]+=d; c[k]++} END{for(k in s) printf "%.2f %d %s\n", s[k]/100/'$el', c[k], k}' thr0 thr1 | sort -rn | head -15
echo "== R/D thread samples by process+state (count over all rounds)"
awk '{print $1, $2}' samples | sort | uniq -c | sort -rn | head -15
echo "== D-state (blocked on I/O) by process + kernel wait function"
awk '$2=="D"{print $1, $3}' samples | sort | uniq -c | sort -rn | head -25
echo "== D-state by process + syscall nr (1=write 18=pwrite64 74=fsync 75=fdatasync 26=msync 0=read 17=pread64 -1/running=?)"
awk '$2=="D"{print $1, $4}' samples | sort | uniq -c | sort -rn | head -15
echo "== D-state path args (syscall nr + normalized path; 263=unlinkat 166=umount2 257=openat 258=mkdirat)"
sort paths | uniq -c | sort -rn | head -25
echo "== D-state path args by parent dir"
awk '{n=split($2,p,"/"); d=""; for(i=2;i<n;i++) d=d"/"p[i]; print $1, d}' paths | sort | uniq -c | sort -rn | head -15
echo "== D-state kernel stacks (top 8 frames, innermost first) by process + syscall"
sort stacks | uniq -c | sort -rn | head -25
echo "== per-process I/O over window (MB read, MB written, write syscalls, cancelled MB)"
awk 'NR==FNR{r[$1]+=$2; w[$1]+=$3; s[$1]+=$4; x[$1]+=$5; next} {r2[$1]+=$2; w2[$1]+=$3; s2[$1]+=$4; x2[$1]+=$5} END{for(k in r2) printf "%s read=%.1fMB write=%.1fMB syscw=%d cancelled=%.1fMB\n", k, (r2[k]-r[k])/1e6, (w2[k]-w[k])/1e6, s2[k]-s[k], (x2[k]-x[k])/1e6}' io0 io1 | sort -t= -k3 -rn | head -15
echo "== vmstat deltas (pages)"
paste vm0 vm1 | awk '{print $1, "start="$2, "end="$4, "delta="$4-$2}'
echo "== diskstats deltas (dev: reads writes ms_reading ms_writing in_flight ms_io weighted_ms)"
awk 'NR==FNR{k=$3; r[k]=$4; w[k]=$8; mr[k]=$7; mw[k]=$11; io[k]=$13; wt[k]=$14; next} ($3 in r) && ($3 ~ /^(nvme[0-9]+n[0-9]+|sd[a-z])$/){dw=$8-w[$3]; lat=0; if (dw>0) lat=($11-mw[$3])/dw; printf "%s reads=%d writes=%d ms_r=%d ms_w=%d inflight=%d ms_io=%d wms=%d avg_w_lat_ms=%.2f\n", $3, $4-r[$3], dw, $7-mr[$3], $11-mw[$3], $12, $13-io[$3], $14-wt[$3], lat}' disk0 disk1
echo "== pressure"; for r in cpu io memory; do echo "$r: $(cat /proc/pressure/$r 2>/dev/null | tr '\n' ' ')"; done
echo "== host disk and inodes (/var/lib/ate)"
df -k /proc/1/root/var/lib/ate 2>/dev/null | tail -1; df -i /proc/1/root/var/lib/ate 2>/dev/null | tail -1
echo "== host ARP entries / gc_thresh3: $(($(wc -l < /proc/1/net/arp 2>/dev/null) - 1)) / $(cat /proc/sys/net/ipv4/neigh/default/gc_thresh3 2>/dev/null)"
echo "== host netns count (/run/netns): $(ls /proc/1/root/run/netns 2>/dev/null | wc -l)"
echo "== mount table size at end"
grep -E ' (ateom|ateworker|atelet)' pids | while read pid comm; do echo "$comm pid=$pid mounts=$(wc -l < /proc/$pid/mountinfo 2>/dev/null)"; done
wait $THR_PID 2>/dev/null
echo "== actor CPU throttling (gVisor sandbox cgroups, 5 s deltas summed over window)"
awk '{per+=$1; thr+=$2; tu+=$3; us+=$4} END{if(per>0) printf "periods=%d throttled=%d throttled_pct=%.1f throttled_sec=%.1f usage_sec=%.1f throttled_share_of_usage=%.1f%%\n", per, thr, 100*thr/per, tu/1e6, us/1e6, (us>0?100*tu/us:0); else print "no sandbox cgroups sampled"}' thr_deltas 2>/dev/null
echo "sandbox cgroups per sample (min/avg/max): $(awk '{s+=$1; if(NR==1||$1<mn)mn=$1; if($1>mx)mx=$1} END{if(NR) printf "%d/%.0f/%d", mn, s/NR, mx}' thr_counts 2>/dev/null)"
echo "cpu.max quota values in last sample (count value): $(tail -1 thr_max 2>/dev/null)"
echo "== probe end $(date -u +%T)"
