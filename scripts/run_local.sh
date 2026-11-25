#!/bin/bash
# =====================================================================
#  run_local.sh
#  Prove in modalità LOCALE (standalone):
#    - i dati vengono letti dal disco del Mac, niente HDFS (-fs file:///)
#    - i job girano col LocalJobRunner, niente YARN (-jt local)
#  Non serve aver acceso né HDFS né YARN.
#  Esegue Job1, Job2, Job3 su in1, in2, in3 e salva i tempi e i
#  risultati, una cartella per ogni job e input.
#
#  Uso (dalla cartella utubbist):
#      ./scripts/run_local.sh            # 1 ripetizione per prova
#      REPS=3 ./scripts/run_local.sh     # 3 ripetizioni per prova
#
#  Risultati in  ~/Desktop/bigDataProj1/out/local/<data_ora>/
#      tempi.csv                  tempi di tutte le prove
#      log/jobN_inM_runR.log      log di ogni prova
#      risultati/jobN/inM/        output del job (part-r-00000)
# =====================================================================

set -u            # errore se uso una variabile non definita

MODALITA=local

# ---------- 1. Percorsi --------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJ_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"          # .../utubbist
BASE_DIR="$(cd "$PROJ_DIR/.." && pwd)"            # .../bigDataProj1
DATA_DIR="$BASE_DIR/youtube-new"                  # CSV e JSON locali

JAR="$(ls "$PROJ_DIR"/target/utubbist-*.jar 2>/dev/null | head -1)"

# In locale gli input sono direttamente le cartelle sul disco
# (in1, in2, in3 contengono hard link ai CSV: nessuna copia).
JSON_DIR="$DATA_DIR/json"

REPS="${REPS:-1}"

STAMP="$(date +%Y%m%d_%H%M%S)"
RES_DIR="$BASE_DIR/out/$MODALITA/$STAMP"
LOG_DIR="$RES_DIR/log"
RIS_DIR="$RES_DIR/risultati"
CSV="$RES_DIR/tempi.csv"

now_ms() { perl -MTime::HiRes=time -e 'printf("%d\n", time()*1000)'; }

# ---------- 2. Controlli iniziali ---------------------------------------
command -v hadoop >/dev/null || { echo "ERRORE: comando 'hadoop' non trovato nel PATH"; exit 1; }
[ -n "$JAR" ] || { echo "ERRORE: jar non trovato in $PROJ_DIR/target. Lancia prima: mvn clean package"; exit 1; }
for d in in1 in2 in3 json; do
    [ -d "$DATA_DIR/$d" ] || { echo "ERRORE: manca la cartella $DATA_DIR/$d"; exit 1; }
done

# ---------- 3. Esecuzione delle prove -----------------------------------
mkdir -p "$LOG_DIR" "$RIS_DIR"
echo "job,input,run,tempo_job_ms,tempo_wall_ms,esito" > "$CSV"

echo ">> Jar: $JAR"
echo ">> Risultati in: $RES_DIR"

for J in 1 2 3; do
    CLASSE="it.bigdata.job$J.Driver$J"
    for IN in in1 in2 in3; do
        INPUT="$DATA_DIR/$IN"
        # Il job scrive direttamente qui: il Driver cancella la cartella se
        # esiste già, quindi con REPS>1 resta l'output dell'ultima ripetizione
        OUTPUT="$RIS_DIR/job$J/$IN"
        mkdir -p "$RIS_DIR/job$J"

        EXTRA=""
        [ "$J" = 3 ] && EXTRA="$JSON_DIR"

        for R in $(seq 1 "$REPS"); do
            LOG="$LOG_DIR/job${J}_${IN}_run$R.log"
            echo ""
            echo "=== Job$J su $IN (run $R/$REPS) ==="

            T0=$(now_ms)
            # -fs file:///  => file system locale invece di hdfs://localhost:9000
            # -jt local     => LocalJobRunner invece di YARN
            # i percorsi file:// li rendono espliciti anche per il Mapper3
            hadoop jar "$JAR" "$CLASSE" \
                -fs file:/// -jt local \
                "file://$INPUT" "file://$OUTPUT" ${EXTRA:+"file://$EXTRA"} > "$LOG" 2>&1
            CODICE=$?
            T1=$(now_ms)
            WALL=$((T1 - T0))

            TJOB=$(grep '^TEMPO_MS' "$LOG" | tail -1 | awk '{print $2}')
            [ -z "$TJOB" ] && TJOB="NA"

            if [ $CODICE -eq 0 ]; then ESITO=OK; else ESITO="ERRORE($CODICE)"; fi

            echo "job$J,$IN,$R,$TJOB,$WALL,$ESITO" >> "$CSV"
            echo "    tempo job = $TJOB ms | wall-clock = $WALL ms | $ESITO"
            [ $CODICE -ne 0 ] && echo "    guarda il log: $LOG"
        done
    done
done

# ---------- 4. Riepilogo ------------------------------------------------
echo ""
echo "================ RIEPILOGO ================"
column -s, -t < "$CSV" 2>/dev/null || cat "$CSV"
echo ""
echo "CSV dei tempi:  $CSV"
echo "Log delle prove: $LOG_DIR"
echo "Risultati:       $RIS_DIR/jobN/inM/"
