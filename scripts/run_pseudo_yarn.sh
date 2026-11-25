#!/bin/bash
# =====================================================================
#  run_pseudo_yarn.sh
#  Prove in modalità PSEUDO-DISTRIBUITA CON YARN:
#    - i dati stanno su HDFS (hdfs://localhost:9000)
#    - i job vengono mandati a YARN (mapreduce.framework.name=yarn):
#      ResourceManager assegna i container, NodeManager li esegue
#  Esegue Job1, Job2, Job3 su in1, in2, in3, salva i tempi e copia
#  sul Mac i risultati, una cartella per ogni job e input.
#
#  Uso (dalla cartella utubbist):
#      ./scripts/run_pseudo_yarn.sh            # 1 ripetizione per prova
#      REPS=3 ./scripts/run_pseudo_yarn.sh     # 3 ripetizioni per prova
#
#  Risultati in  ~/Desktop/bigDataProj1/out/pseudo_yarn/<data_ora>/
#      tempi.csv                  tempi di tutte le prove
#      log/jobN_inM_runR.log      log di ogni prova
#      risultati/jobN/inM/        output del job (part-r-00000)
# =====================================================================

set -u            # errore se uso una variabile non definita

MODALITA=pseudo_yarn

# ---------- 1. Percorsi --------------------------------------------------
# Cartella dello script -> cartella del progetto (utubbist) -> bigDataProj1
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJ_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"          # .../utubbist
BASE_DIR="$(cd "$PROJ_DIR/.." && pwd)"            # .../bigDataProj1
DATA_DIR="$BASE_DIR/youtube-new"                  # CSV e JSON locali

# Il jar prodotto da "mvn package"
JAR="$(ls "$PROJ_DIR"/target/utubbist-*.jar 2>/dev/null | head -1)"

# Cartelle su HDFS
HDFS_ALL=/yt/in3              # i 10 CSV (caricati UNA volta sola)
HDFS_JSON=/yt/json            # i 10 JSON delle categorie
HDFS_OUT=/out/$MODALITA       # qui il job scrive: /out/pseudo_yarn/jobN/inM

# in1 e in2 NON vengono ricaricati: sono "glob" sui file di /yt/in3.
# FileInputFormat espande da solo le graffe {..}, quindi il job legge
# solo i file elencati. Così su HDFS i CSV occupano spazio una volta sola
# (il disco del Mac è quasi pieno).
INPUT_in1="$HDFS_ALL/{US,GB,DE}videos.csv"
INPUT_in2="$HDFS_ALL/{US,GB,DE,CA,FR,RU}videos.csv"
INPUT_in3="$HDFS_ALL"

REPS="${REPS:-1}"       # ripetizioni per ogni prova (default 1)

# Cartella dei risultati sul Mac: una nuova per ogni lancio, con data e ora,
# così un lancio non sovrascrive mai quello precedente
STAMP="$(date +%Y%m%d_%H%M%S)"
RES_DIR="$BASE_DIR/out/$MODALITA/$STAMP"
LOG_DIR="$RES_DIR/log"
RIS_DIR="$RES_DIR/risultati"
CSV="$RES_DIR/tempi.csv"

# Millisecondi da epoch (date +%s%N non esiste su macOS, perl sì)
now_ms() { perl -MTime::HiRes=time -e 'printf("%d\n", time()*1000)'; }

# ---------- 2. Controlli iniziali ---------------------------------------
command -v hadoop >/dev/null || { echo "ERRORE: comando 'hadoop' non trovato nel PATH"; exit 1; }
[ -n "$JAR" ] || { echo "ERRORE: jar non trovato in $PROJ_DIR/target. Lancia prima: mvn clean package"; exit 1; }

echo ">> Controllo che HDFS sia acceso..."
if ! hdfs dfs -ls / >/dev/null 2>&1; then
    echo "ERRORE: HDFS non risponde. Avvialo con: start-dfs.sh  (e controlla con: jps)"
    exit 1
fi

# Appena acceso, il NameNode resta qualche secondo in "safe mode" (sola
# lettura) e i job che scrivono falliscono. Questo comando aspetta che esca.
echo ">> Aspetto che HDFS esca dalla safe mode..."
hdfs dfsadmin -safemode wait

# YARN: jps elenca i processi Java attivi. Servono ResourceManager e NodeManager.
echo ">> Controllo che YARN sia acceso..."
JPS="$(jps 2>/dev/null)"
if ! echo "$JPS" | grep -q ResourceManager || ! echo "$JPS" | grep -q NodeManager; then
    echo "ERRORE: YARN non è attivo (manca ResourceManager o NodeManager)."
    echo "        Avvialo con: start-yarn.sh  e ricontrolla con: jps"
    exit 1
fi

# ---------- 3. Caricamento dati su HDFS (solo se mancano) ---------------
carica_se_manca() {      # $1 = cartella locale, $2 = cartella HDFS
    if hdfs dfs -test -d "$2"; then
        echo ">> $2 già presente su HDFS, non ricarico"
    else
        echo ">> Carico $1 -> $2"
        hdfs dfs -mkdir -p "$2"
        hdfs dfs -put "$1"/* "$2"/ || { echo "ERRORE nel caricamento di $2"; exit 1; }
    fi
}
carica_se_manca "$DATA_DIR/in3"  "$HDFS_ALL"
carica_se_manca "$DATA_DIR/json" "$HDFS_JSON"

# ---------- 4. Esecuzione delle prove -----------------------------------
mkdir -p "$LOG_DIR" "$RIS_DIR"
echo "job,input,run,tempo_job_ms,tempo_wall_ms,esito,application_id" > "$CSV"

echo ">> Jar: $JAR"
echo ">> Risultati in: $RES_DIR"

for J in 1 2 3; do
    CLASSE="it.bigdata.job$J.Driver$J"
    for IN in in1 in2 in3; do
        # prende il valore di INPUT_in1 / INPUT_in2 / INPUT_in3
        VAR="INPUT_$IN"
        INPUT="${!VAR}"
        OUTPUT="$HDFS_OUT/job$J/$IN"          # output su HDFS
        LOCALE="$RIS_DIR/job$J/$IN"           # dove lo copio sul Mac
        hdfs dfs -mkdir -p "$HDFS_OUT/job$J"  # la cartella padre deve esistere

        # Job3 vuole come terzo argomento la cartella dei JSON
        EXTRA=""
        [ "$J" = 3 ] && EXTRA="$HDFS_JSON"

        for R in $(seq 1 "$REPS"); do
            LOG="$LOG_DIR/job${J}_${IN}_run$R.log"
            echo ""
            echo "=== Job$J su $IN (run $R/$REPS) ==="

            T0=$(now_ms)
            # -D mapreduce.framework.name=yarn  => il job va a YARN
            # (lo scrivo esplicito così non dipende da mapred-site.xml)
            # $EXTRA senza virgolette: se è vuoto sparisce
            hadoop jar "$JAR" "$CLASSE" \
                -D mapreduce.framework.name=yarn \
                "$INPUT" "$OUTPUT" $EXTRA > "$LOG" 2>&1
            CODICE=$?
            T1=$(now_ms)
            WALL=$((T1 - T0))

            # Il Driver stampa "TEMPO_MS <n>": prendo il numero
            TJOB=$(grep '^TEMPO_MS' "$LOG" | tail -1 | awk '{print $2}')
            [ -z "$TJOB" ] && TJOB="NA"

            # Id dell'applicazione YARN (utile per ritrovarla su http://localhost:8088)
            APP=$(grep -o 'application_[0-9]*_[0-9]*' "$LOG" | head -1)
            [ -z "$APP" ] && APP="NA"

            if [ $CODICE -eq 0 ]; then ESITO=OK; else ESITO="ERRORE($CODICE)"; fi

            echo "job$J,$IN,$R,$TJOB,$WALL,$ESITO,$APP" >> "$CSV"
            echo "    tempo job = $TJOB ms | wall-clock = $WALL ms | $ESITO | $APP"
            [ $CODICE -ne 0 ] && echo "    guarda il log: $LOG"

            # Copio il risultato da HDFS al Mac (fuori dal tempo misurato).
            # Lo faccio una volta sola: le ripetizioni danno lo stesso output.
            if [ $CODICE -eq 0 ] && [ ! -d "$LOCALE" ]; then
                mkdir -p "$RIS_DIR/job$J"
                hdfs dfs -get "$OUTPUT" "$LOCALE"
            fi
        done
    done
done

# ---------- 5. Riepilogo ------------------------------------------------
echo ""
echo "================ RIEPILOGO ================"
column -s, -t < "$CSV" 2>/dev/null || cat "$CSV"
echo ""
echo "CSV dei tempi:  $CSV"
echo "Log delle prove: $LOG_DIR"
echo "Risultati:       $RIS_DIR/jobN/inM/"
