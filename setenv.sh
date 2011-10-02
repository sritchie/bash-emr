# # you must set EMR_HOME to point to the root directory of your 'elastic-mapreduce-ruby' install

export PATH=$EMR_HOME:$PATH

# EMR helpers
KEYPAIR=`cat $EMR_HOME/credentials.json | grep key-pair-file | cut -d':' -f2 | sed -n 's|.*"\([^"]*\)".*|\1|p'`
alias essh="ssh -i $KEYPAIR -o StrictHostKeyChecking=no -o ServerAliveInterval=30"
alias escp="scp -i $KEYPAIR -o StrictHostKeyChecking=no -o ServerAliveInterval=30"

function emr {
  RESULT=`elastic-mapreduce $*`
  ID=`echo "$RESULT" | head -1 | sed -n 's|^Cr.*\(j-[^ ]*\)$|\1|p'`
  
  [ -n "$ID" ] && export EMR_FLOW_ID="$ID"
  
  echo "$RESULT"
}

function emrattach {
  if [ -z "$1" ]; then
    if [ -z "$EMR_FLOW_ID" ]; then
      echo "Not currently attached to any EMR cluster."
    else
      listing=`emr --list`
      clustername=`echo $listing  | grep $EMR_FLOW_ID | cut -d' ' -f4`
      echo "Currently attached to $clustername."
    fi
  else
    flowid=`emr --list | grep $1 | cut -d' ' -f1`
    if [ -z "$flowid" ]; then
      echo "Couldn't attach; cluster named $1 doesn't exist!"
    else
      export EMR_FLOW_ID=$flowid
      echo "Successfully attached to $1."
    fi
  fi
}

function emrset {
  if [ -z "$1" ]; then
    echo $EMR_FLOW_ID
  else
    export EMR_FLOW_ID=$1
  fi
}

function flowid {
  if [ -z "$EMR_FLOW_ID" ]; then
    echo "$1"
  else
    echo "$EMR_FLOW_ID"
  fi
}

function emrhost {
  FLOW_ID=`flowid $1`
  unset H
  while [ -z "$H" ]; do
   H=`emr -j $FLOW_ID --describe | grep MasterPublicDnsName | sed -n 's|.*"\([^"]*.amazonaws.com\)".*|\1|p'`
   sleep 5
  done
  echo $H
}

function emrscreen {
 HOST=`emrhost $1`
 essh -t "hadoop@$HOST" 'screen -s -$SHELL -D -R'
}

function emrtail {
  if [ -z "$1" ]; then
    echo "Must provide step number to tail!"
    HOST=`emrhost $HH`
    essh -t "hadoop@$HOST" "ls -1 /mnt/var/log/hadoop/steps/"
    return
  fi
      
  if [ $# == 2 ]; then
    HH=$1
    STEP=$2
  else
    HH=""
    STEP=$1
  fi   
  HOST=`emrhost $HH`
  essh -t "hadoop@$HOST" "tail -100f /mnt/var/log/hadoop/steps/$STEP/syslog"
}

function emrlogin {
 HOST=`emrhost $1`
 essh "hadoop@$HOST"
}
 
function emrproxy {
 HOST=`emrhost $1`
 echo http://$HOST:9100
 essh -D 6666 -N "hadoop@$HOST"
}

function emrstat {
 FLOW_ID=`flowid $1`
 emr -j $FLOW_ID --describe | grep 'LastStateChangeReason' | head -1 | cut -d":" -f2 | sed -n 's|^ "\([^\"]*\)".*|\1|p'
}

function emrterminate {
 FLOW_ID=`flowid $1`
 emr -j $FLOW_ID --terminate
 export EMR_FLOW_ID=""
}

function emrscp {
 HOST=`emrhost`
 escp $1 "hadoop@$HOST:"
}
