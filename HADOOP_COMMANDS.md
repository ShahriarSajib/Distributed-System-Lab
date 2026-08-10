# Hadoop Built-in Commands — Complete Reference

Everything you can do with Hadoop's **built-in (shell) commands** — no custom code needed.
For the 50 custom MapReduce lab programs, see `RUN.md` and `EXAM_GUIDE.md`.

---

## Table of Contents

1. [The 5 command families](#1-the-5-command-families)
2. [`hdfs dfs` / `hadoop fs` — filesystem commands](#2-hdfs-dfs--hadoop-fs--filesystem-commands)
3. [`hadoop jar` / `yarn jar` — run a MapReduce job](#3-hadoop-jar--yarn-jar--run-a-mapreduce-job)
4. [`yarn` — cluster, apps, containers, logs](#4-yarn--cluster-apps-containers-logs)
5. [`mapred` — job history and job control](#5-mapred--job-history-and-job-control)
6. [Cluster lifecycle scripts (sbin/)](#6-cluster-lifecycle-scripts-sbin)
7. [Info & admin commands](#7-info--admin-commands)
8. [Can I do the existing lab tasks with built-in commands?](#8-can-i-do-the-existing-lab-tasks-with-built-in-commands)
9. [One-line cheat sheet](#9-one-line-cheat-sheet)

---

## 1. The 5 command families

| Command | What it does | When you need it |
|---|---|---|
| `hdfs dfs ...` | HDFS file operations (list, put, get, cat, rm, mkdir) | Managing input/output files — **used constantly** |
| `hadoop fs ...` | Identical to `hdfs dfs` | Old style, same commands, pick one |
| `hadoop jar <jar> <Class> <in> <out>` | Runs a MapReduce jar | Running the 50 lab programs |
| `yarn ...` | Cluster info, applications, containers, logs | Check a job is running, kill it, read logs |
| `mapred ...` | MapReduce-specific tools, job history | View finished jobs, queue info |
| scripts in `/usr/local/hadoop/sbin/` | Start / stop daemons | Cluster lifecycle |

> `hadoop fs` and `hdfs dfs` are the same thing — `hdfs dfs` is the modern form. This guide
> uses `hdfs dfs`, but every line works with `hadoop fs` too.

---

## 2. `hdfs dfs` / `hadoop fs` — filesystem commands

This is the command you will type **most**. All paths below are HDFS paths (`/lab/...`).

### 2.1 Navigate & inspect

```bash
hdfs dfs -ls /lab                         # list files in a directory
hdfs dfs -ls -R /lab                      # recursive listing (all subdirs)
hdfs dfs -ls -h /lab/input                # human-readable sizes (K/M/G)
hdfs dfs -find /lab -name '*.csv'         # search for files by name
hdfs dfs -du /lab                         # sizes of each entry
hdfs dfs -du -h -s /lab/input             # total size of a folder (-s = summary)
hdfs dfs -df -h /                         # free/used space of HDFS
hdfs dfs -stat %n %b %o /lab/input/input.txt   # name, size, block size of a file
hdfs dfs -count /lab                      # file/dir counts per path
```

### 2.2 Create directories & files

```bash
hdfs dfs -mkdir /lab/input                # make one directory
hdfs dfs -mkdir -p /lab/a/b/c             # -p = create parents too (like mkdir -p)
hdfs dfs -touchz /lab/input/empty.txt     # create an empty 0-byte file
hdfs dfs -touch /lab/input/input.txt      # update a file's timestamp (or create it)
```

### 2.3 Transfer files (local ↔ HDFS)

```bash
# upload a file
hdfs dfs -put input.txt /lab/input/              # copy local -> HDFS
hdfs dfs -put -f input.txt /lab/input/           # -f overwrite (used in RUN.md)
hdfs dfs -copyFromLocal input.txt /lab/input/    # same as -put
hdfs dfs -moveFromLocal input.txt /lab/input/    # copy then DELETE local file

# download a file
hdfs dfs -get /lab/input/input.txt ./            # HDFS -> local (same as -copyToLocal)
hdfs dfs -get /lab/lw-out/part-r-00000 ./result.txt
hdfs dfs -getmerge /lab/lw-out ./all.txt         # merge ALL part files into one local file
hdfs dfs -copyToLocal /lab/input/input.txt ./    # same as -get

# append local data onto an HDFS file
hdfs dfs -appendToFile notes.txt /lab/input/input.txt
```

> **`-getmerge` is the lab favorite** — after a run, it merges all `part-r-*` output
> files into one local text file so you can open it in a normal editor.

### 2.4 Copy, move, delete

```bash
hdfs dfs -cp /lab/input/input.txt /lab/backup.txt     # copy inside HDFS
hdfs dfs -mv /lab/backup.txt /lab/input/input.txt     # move / rename
hdfs dfs -rm /lab/input/empty.txt                     # delete one file
hdfs dfs -rm -r /lab/lw-out                           # delete a directory + contents
hdfs dfs -rm -r -f /lab/lw-out                        # -f = don't ask, force (used in RUN.md)
hdfs dfs -rmdir /lab/a/b/c                            # delete EMPTY directory only
hdfs dfs -expunge                                      # clear the trash (skipped on -skipTrash)
```

> **Rule from `RUN.md`:** always `hdfs dfs -rm -r -f /lab/<name>-out` before re-running a
> program, or Hadoop refuses with `Output directory already exists`.

### 2.5 View file contents

```bash
hdfs dfs -cat /lab/input/input.txt          # print whole file to screen
hdfs dfs -cat /lab/lw-out/part-r-00000      # print a MapReduce result
hdfs dfs -text /lab/log-out/part-r-00000    # like -cat, but also reads gzip/sequence files
hdfs dfs -tail /lab/input/input.txt         # last kilobyte of a file
hdfs dfs -head /lab/input/input.txt         # first kilobyte of a file
hdfs dfs -checksum /lab/input/input.txt     # MD5-like checksum to verify integrity
```

### 2.6 Permissions & ownership

```bash
hdfs dfs -chmod 755 /lab/input/input.txt        # change permissions
hdfs dfs -chmod -R 644 /lab/input               # -R recursive
hdfs dfs -chown sajib:hadoop /lab/input         # change owner:group
hdfs dfs -chgrp hadoop /lab/input               # change group only
hdfs dfs -getfacl /lab/input/input.txt          # read ACLs
hdfs dfs -setfacl -m user:alice:r-- /lab/x      # add an ACL entry
```

### 2.7 Replication & misc

```bash
hdfs dfs -setrep -w 3 /lab/input/input.txt      # set replication factor to 3 (-w = wait)
hdfs dfs -test -d /lab/input && echo exists      # test: -d dir, -f file, -z zero size, -e exists
hdfs dfs -concat /lab/big.txt /lab/p1.txt /lab/p2.txt   # append several HDFS files into one
```

### 2.8 Snapshots (safe rollback of a directory)

```bash
hdfs dfsadmin -allowSnapshot /lab/input          # enable snapshots on a dir (admin)
hdfs dfs -createSnapshot /lab/input snap1        # take a snapshot
hdfs dfs -ls /lab/input/.snapshot/snap1          # browse it
hdfs dfs -renameSnapshot /lab/input snap1 snap2  # rename it
hdfs dfs -deleteSnapshot /lab/input snap2        # delete it
```

---

## 3. `hadoop jar` / `yarn jar` — run a MapReduce job

This is how **every lab program** in `RUN.md` is executed.

```bash
hadoop jar build/jars/WordCount.jar WordCount /lab/input /lab/wc-out
#        ^jar file                  ^main class  ^input dir  ^output dir
```

```bash
# the modern form (recommended by Hadoop for YARN apps):
yarn jar build/jars/LongestWord.jar LongestWord /lab/input /lab/lw-out
```

- `hadoop jar <jar> [mainClass] args...` — `mainClass` is only needed when the jar has
  several mains (every `build/jars/*.jar` in this lab has exactly one, but it is still
  passed because the class is not declared in the manifest).
- Output directory **must not exist** → delete it first:
  ```bash
  hdfs dfs -rm -r -f /lab/wc-out
  ```
- The output is a **directory** containing `part-r-00000`, `part-r-00001`, ... (reducers)
  or `part-m-00000`, ... (map-only jobs like `UpperCaseConverter`).

```bash
# check the result
hdfs dfs -cat /lab/wc-out/part-r-00000
```

---

## 4. `yarn` — cluster, apps, containers, logs

After a job is submitted you use `yarn` to watch it.

```bash
yarn node -list                        # all worker nodes + status
yarn cluster -list                     # cluster metrics (jobs running, memory used)
yarn application -list                 # currently running applications
yarn application -list -appStates ALL  # all applications (also FINISHED/FAILED)
yarn application -status <app-id>      # full report for one app (state, url, progress)
yarn application -kill <app-id>        # kill a stuck application
yarn applicationattempt -list <app-id> # the attempts of an application
yarn container -list <app-attempt-id>  # containers of an attempt (map/reduce tasks)
yarn logs -applicationId <app-id>      # dump ALL container logs for a job
yarn logs -applicationId <app-id> -log_files stdout   # just stdout logs
yarn top                               # live view of the cluster (like `top`)
yarn queue -status default             # queue status
```

> `yarn application -status <app_id>` is the first thing to run when a job hangs or fails.
> It shows `State` and a link to the YARN web UI (ResourceManager on port `8088`).

---

## 5. `mapred` — job history and job control

```bash
mapred job -list all                    # list all jobs (old API, useful after MapReduce v1)
mapred job -status <job-id>             # status of one job
mapred job -kill <job-id>               # kill a job
mapred job -history <job-output-dir>    # full history from a job's output dir
mapred queue -list                      # available queues
mapred distcp -update hdfs://a/data hdfs://b/data   # cluster-to-cluster copy
mapred archive -archiveName files.har -p /lab /lab/har   # bundle many files into a HAR archive
mapred classpath                        # classpath needed for mapreduce
```

---

## 6. Cluster lifecycle scripts (sbin/)

Located in `/usr/local/hadoop/sbin/`.

```bash
start-dfs.sh            # start NameNode + DataNodes (HDFS)
start-yarn.sh           # start ResourceManager + NodeManagers
start-all.sh            # both of the above (or use start-dfs.sh + start-yarn.sh)
stop-dfs.sh             # stop HDFS daemons
stop-yarn.sh            # stop YARN daemons
stop-all.sh             # stop everything
jps                     # list Java processes -> NameNode, DataNode, ResourceManager, NodeManager
hadoop-daemon.sh start namenode     # start one specific daemon
hadoop-daemon.sh stop datanode      # stop one specific daemon
```

> `start-dfs.sh && start-yarn.sh && jps` is the session-start command used in `RUN.md`.
> Expected output of `jps`: `NameNode`, `DataNode`, `ResourceManager`, `NodeManager`
> (plus `SecondaryNameNode` and the current shell's jps itself).

---

## 7. Info & admin commands

```bash
hadoop version                    # Hadoop version + build info
hadoop classpath                  # full classpath (handy for javac -cp in build_all.sh)
hadoop checknative                # are native libs (compression) available?
hadoop fs -help                   # help for all fs commands
hadoop fs -help cat               # help for one fs command
hdfs dfsadmin -report            # NameNode report: capacity, DNs alive, replication
hdfs dfsadmin -safemode get      # is HDFS in safe mode?
hdfs dfsadmin -safemode leave    # force-leave safe mode (read-only at startup)
hdfs dfsadmin -allowSnapshot /lab/input   # enable snapshots
hdfs balancer                     # balance data across nodes
hdfs fsck /lab                   # check file health (missing/corrupt blocks)
```

---

## 8. Can I do the existing lab tasks with built-in commands?

**Short answer: mostly NO for the computations, YES for everything around them.**

The 50 lab tasks (word count, averages, max per department, ...) are **computations**.
Built-in Hadoop commands do **not** calculate anything — they move, list, view and delete
files. Real summing/counting requires MapReduce → that is exactly why you have the jars.

### What you CAN do with built-ins (very useful in the exam)

| Task | Command |
|---|---|
| Is my input uploaded? | `hdfs dfs -ls /lab/input` |
| Peek at the data before writing code | `hdfs dfs -cat /lab/input/input.txt` |
| See how big the data is | `hdfs dfs -du -h -s /lab/input` |
| Check HDFS disk free | `hdfs dfs -df -h /` |
| Delete old output before re-run | `hdfs dfs -rm -r -f /lab/<name>-out` |
| See output file names | `hdfs dfs -ls /lab/<name>-out` |
| Download one result file | `hdfs dfs -get /lab/wc-out/part-r-00000 ./out.txt` |
| **Merge all result parts** | `hdfs dfs -getmerge /lab/wc-out ./out.txt` |
| Watch the running job | `yarn application -status <app-id>` |
| See why a job failed | `yarn logs -applicationId <app-id>` |
| Count words without MapReduce* | `hdfs dfs -cat /lab/input/input.txt \| tr ' ' '\n' \| sort \| uniq -c` |
| Verify a re-upload really changed | `hdfs dfs -checksum /lab/input/input.txt` |

\* The last row is **not** a Hadoop command — it is a Linux shell pipeline that reads HDFS
data via `hdfs dfs -cat`. It works for tiny exam inputs but is not "distributed" and will
not scale. For a real exam answer, run the MapReduce jar.

### What built-ins CANNOT do

- sum / average / max / min / count over the data
- group-by (per year, per department, per domain, per IP)
- sort the data by value
- filter lines by a condition
- transform text (lowercase, uppercase, reverse)

Every one of these needs a Mapper + Reducer → use one of the 50 jars in `build/jars/`
or the templates in `EXAM_GUIDE.md` (Steps 3–5).

---

## 9. One-line cheat sheet

```bash
# --- cluster ---
start-dfs.sh && start-yarn.sh && jps              # boot cluster + verify daemons
stop-all.sh                                       # shut down

# --- HDFS basics ---
hdfs dfs -mkdir -p /lab/input                     # make dirs
hdfs dfs -put -f input.txt /lab/input/            # upload (overwrite)
hdfs dfs -ls -R /lab                              # list everything
hdfs dfs -cat /lab/input/input.txt                # view a file
hdfs dfs -tail /lab/input/input.txt               # view the end
hdfs dfs -rm -r -f /lab/out                       # delete old output
hdfs dfs -getmerge /lab/out ./result.txt          # download all parts as one file
hdfs dfs -du -h -s /lab/input                     # folder size
hdfs dfs -df -h /                                 # free space
hdfs dfs -cp -mv /lab/x /lab/y                    # copy / move
hdfs dfs -chmod -R 644 /lab/input                 # permissions

# --- run a job ---
hadoop jar build/jars/WordCount.jar WordCount /lab/input /lab/wc-out
# or:  yarn jar build/jars/WordCount.jar WordCount /lab/input /lab/wc-out

# --- watch / debug a job ---
yarn application -list                            # running apps
yarn application -status <app-id>                 # status of one
yarn application -kill <app-id>                   # kill it
yarn logs -applicationId <app-id>                 # container logs

# --- admin ---
hadoop version                                    # version info
hdfs dfsadmin -report                             # cluster health
hdfs fsck /lab                                    # file health
```
