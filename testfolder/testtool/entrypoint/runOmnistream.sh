#LOG_FILE="${OMNISTREAM_HOME}/testtool/queryfold/query0_OmniStream/q0_run.log"
TARGET_FOLDER="${QUERY_LOGS}"
LOG_FILE="${QUERY_LOGS}/q0_run.log"

read -p "please enter the  query ID (0 to 22)" input
echo "we are going to run queries: $input"
query_set=",$input,"

if [ -z "$input" ]; then
    run_all=true
else
    run_all=false
fi

if $run_all || [[ "$query_set" == *",0,"* ]]; then
    echo "start running query0 at time: $(date +"%H:%M:%S")"
    ${OMNISTREAM_HOME}/testtool/queryfold/query0_OmniStream/run_q0.sh>> "$LOG_FILE" 2>&1
    TXTFILE="/tmp/flink_output.txt"
    if [ -f "$TXTFILE" ]; then
      cp /tmp/flink_output.txt ${OMNISTREAM_HOME}/testtool/queryfold/query0_OmniStream

      DEST_FOLDER="${QUERY_LOGS}/query0"
      mkdir -p "$DEST_FOLDER"

      cp /tmp/flink_output.txt "${DEST_FOLDER}/"
      cp ${OMNISTREAM_HOME}/testtool/queryfold/query0_flink/query0_result_flink.txt "${DEST_FOLDER}/"

    fi
    python3 ${OMNISTREAM_HOME}/testtool/queryfold/query0_OmniStream/q0logextractor.py
    python3 ${OMNISTREAM_HOME}/testtool/queryfold/query0_OmniStream/validate_result0.py
    FILE="${OMNISTREAM_HOME}/testtool/queryfold/query0_OmniStream/flink_output.txt"
    if [ -f "$FILE" ]; then
        rm "$FILE"
    fi
    echo "finish running query0 at time: $(date +"%H:%M:%S")"

    FILE2="${OMNISTREAM_HOME}/testtool/queryfold/query0_OmniStream/q0-taskname-OmniStream.txt"
    if [ -f "$FILE2" ]; then
        rm "$FILE2"
    fi

fi


if $run_all || [[ "$query_set" == *",1,"* ]]; then
    echo "start running query1 at time: $(date +"%H:%M:%S")"
    ${OMNISTREAM_HOME}/testtool/queryfold/query1_OmniStream/run_q1.sh>> "$LOG_FILE" 2>&1
    TXTFILE="/tmp/flink_output.txt"
    if [ -f "$TXTFILE" ]; then
      cp /tmp/flink_output.txt ${OMNISTREAM_HOME}/testtool/queryfold/query1_OmniStream

            DEST_FOLDER="${QUERY_LOGS}/query1"
            mkdir -p "$DEST_FOLDER"

            cp /tmp/flink_output.txt "${DEST_FOLDER}/"
            cp ${OMNISTREAM_HOME}/testtool/queryfold/query1_flink/query1_result_flink.txt "${DEST_FOLDER}/"

    fi
    python3 ${OMNISTREAM_HOME}/testtool/queryfold/query1_OmniStream/q1logextractor.py
    python3 ${OMNISTREAM_HOME}/testtool/queryfold/query1_OmniStream/validate_result1.py
    FILE="${OMNISTREAM_HOME}/testtool/queryfold/query1_OmniStream/flink_output.txt"
    if [ -f "$FILE" ]; then
        rm "$FILE"
    fi
    echo "finish running query1 at time: $(date +"%H:%M:%S")"
    FILE2="${OMNISTREAM_HOME}/testtool/queryfold/query1_OmniStream/q1-taskname-OmniStream.txt"
    if [ -f "$FILE2" ]; then
        rm "$FILE2"
    fi

fi

if $run_all || [[ "$query_set" == *",2,"* ]]; then
    echo "start running query2 at time: $(date +"%H:%M:%S")"
    ${OMNISTREAM_HOME}/testtool/queryfold/query2_OmniStream/run_q2.sh>> "$LOG_FILE" 2>&1

    TXTFILE="/tmp/flink_output.txt"
    if [ -f "$TXTFILE" ]; then
      cp /tmp/flink_output.txt ${OMNISTREAM_HOME}/testtool/queryfold/query2_OmniStream

            DEST_FOLDER="${QUERY_LOGS}/query2"
            mkdir -p "$DEST_FOLDER"

            cp /tmp/flink_output.txt "${DEST_FOLDER}/"
            cp ${OMNISTREAM_HOME}/testtool/queryfold/query2_flink/query2_result_flink.txt "${DEST_FOLDER}/"

    fi
    python3 ${OMNISTREAM_HOME}/testtool/queryfold/query2_OmniStream/q2logextractor.py
    python3 ${OMNISTREAM_HOME}/testtool/queryfold/query2_OmniStream/validate_result2.py
    FILE="${OMNISTREAM_HOME}/testtool/queryfold/query2_OmniStream/flink_output.txt"
    if [ -f "$FILE" ]; then
        rm "$FILE"
    fi
    echo "finish running query2 at time: $(date +"%H:%M:%S")"

        FILE2="${OMNISTREAM_HOME}/testtool/queryfold/query2_OmniStream/q2-taskname-OmniStream.txt"
        if [ -f "$FILE2" ]; then
            rm "$FILE2"
        fi

fi


if $run_all || [[ "$query_set" == *",3,"* ]]; then
    echo "start running query3 at time: $(date +"%H:%M:%S")"
    ${OMNISTREAM_HOME}/testtool/queryfold/query3_OmniStream/run_q3.sh>> "$LOG_FILE" 2>&1

    TXTFILE="/tmp/flink_output.txt"
    if [ -f "$TXTFILE" ]; then
      cp /tmp/flink_output.txt ${OMNISTREAM_HOME}/testtool/queryfold/query3_OmniStream

            DEST_FOLDER="${QUERY_LOGS}/query3"
            mkdir -p "$DEST_FOLDER"

            cp /tmp/flink_output.txt "${DEST_FOLDER}/"
            cp ${OMNISTREAM_HOME}/testtool/queryfold/query3_flink/query3_result_flink.txt "${DEST_FOLDER}/"

    fi
    python3 ${OMNISTREAM_HOME}/testtool/queryfold/query3_OmniStream/q3logextractor.py
    python3 ${OMNISTREAM_HOME}/testtool/queryfold/query3_OmniStream/validate_result3.py
    FILE="${OMNISTREAM_HOME}/testtool/queryfold/query3_OmniStream/flink_output.txt"
    if [ -f "$FILE" ]; then
        rm "$FILE"
    fi
    echo "finish running query3 at time: $(date +"%H:%M:%S")"

        FILE2="${OMNISTREAM_HOME}/testtool/queryfold/query3_OmniStream/q3-taskname-OmniStream.txt"
        if [ -f "$FILE2" ]; then
            rm "$FILE2"
        fi

fi


if $run_all || [[ "$query_set" == *",4,"* ]]; then
    echo "start running query4 at time: $(date +"%H:%M:%S")"
    ${OMNISTREAM_HOME}/testtool/queryfold/query4_OmniStream/run_q4.sh>> "$LOG_FILE" 2>&1

    TXTFILE="/tmp/flink_output.txt"
    if [ -f "$TXTFILE" ]; then
      cp /tmp/flink_output.txt ${OMNISTREAM_HOME}/testtool/queryfold/query4_OmniStream

            DEST_FOLDER="${QUERY_LOGS}/query4"
            mkdir -p "$DEST_FOLDER"

            cp /tmp/flink_output.txt "${DEST_FOLDER}/"
            cp ${OMNISTREAM_HOME}/testtool/queryfold/query4_flink/query4_result_flink.txt "${DEST_FOLDER}/"

    fi
    python3 ${OMNISTREAM_HOME}/testtool/queryfold/query4_OmniStream/q4logextractor.py
    python3 ${OMNISTREAM_HOME}/testtool/queryfold/query4_OmniStream/validate_result4.py
    FILE="${OMNISTREAM_HOME}/testtool/queryfold/query4_OmniStream/flink_output.txt"
    if [ -f "$FILE" ]; then
        rm "$FILE"
    fi
    echo "finish running query4 at time: $(date +"%H:%M:%S")"


        FILE2="${OMNISTREAM_HOME}/testtool/queryfold/query4_OmniStream/q4-taskname-OmniStream.txt"
        if [ -f "$FILE2" ]; then
            rm "$FILE2"
        fi

fi



if $run_all || [[ "$query_set" == *",5,"* ]]; then
    echo "start running query5 at time: $(date +"%H:%M:%S")"
    ${OMNISTREAM_HOME}/testtool/queryfold/query5_OmniStream/run_q5.sh>> "$LOG_FILE" 2>&1

    TXTFILE="/tmp/flink_output.txt"
    if [ -f "$TXTFILE" ]; then
      cp /tmp/flink_output.txt ${OMNISTREAM_HOME}/testtool/queryfold/query5_OmniStream

            DEST_FOLDER="${QUERY_LOGS}/query5"
            mkdir -p "$DEST_FOLDER"

            cp /tmp/flink_output.txt "${DEST_FOLDER}/"
            cp ${OMNISTREAM_HOME}/testtool/queryfold/query5_flink/query5_result_flink.txt "${DEST_FOLDER}/"

    fi
    python3 ${OMNISTREAM_HOME}/testtool/queryfold/query5_OmniStream/q5logextractor.py
    python3 ${OMNISTREAM_HOME}/testtool/queryfold/query5_OmniStream/validate_result5.py
    FILE="${OMNISTREAM_HOME}/testtool/queryfold/query5_OmniStream/flink_output.txt"
    if [ -f "$FILE" ]; then
        rm "$FILE"
    fi
    echo "finish running query5 at time: $(date +"%H:%M:%S")"


        FILE2="${OMNISTREAM_HOME}/testtool/queryfold/query5_OmniStream/q5-taskname-OmniStream.txt"
        if [ -f "$FILE2" ]; then
            rm "$FILE2"
        fi

fi











if $run_all || [[ "$query_set" == *",7,"* ]]; then
    echo "start running query7 at time: $(date +"%H:%M:%S")"
    ${OMNISTREAM_HOME}/testtool/queryfold/query7_OmniStream/run_q7.sh>> "$LOG_FILE" 2>&1

    TXTFILE="/tmp/flink_output.txt"
    if [ -f "$TXTFILE" ]; then
      cp /tmp/flink_output.txt ${OMNISTREAM_HOME}/testtool/queryfold/query7_OmniStream

            DEST_FOLDER="${QUERY_LOGS}/query7"
            mkdir -p "$DEST_FOLDER"

            cp /tmp/flink_output.txt "${DEST_FOLDER}/"
            cp ${OMNISTREAM_HOME}/testtool/queryfold/query7_flink/query7_result_flink.txt "${DEST_FOLDER}/"

    fi
    python3 ${OMNISTREAM_HOME}/testtool/queryfold/query7_OmniStream/q7logextractor.py
    python3 ${OMNISTREAM_HOME}/testtool/queryfold/query7_OmniStream/validate_result7.py
    FILE="${OMNISTREAM_HOME}/testtool/queryfold/query7_OmniStream/flink_output.txt"
    if [ -f "$FILE" ]; then
        rm "$FILE"
    fi
    echo "finish running query7 at time: $(date +"%H:%M:%S")"
    FILE2="${OMNISTREAM_HOME}/testtool/queryfold/query7_OmniStream/q7-taskname-OmniStream.txt"
    if [ -f "$FILE2" ]; then
        rm "$FILE2"
    fi

fi



if $run_all || [[ "$query_set" == *",8,"* ]]; then
    echo "start running query8 at time: $(date +"%H:%M:%S")"
    ${OMNISTREAM_HOME}/testtool/queryfold/query8_OmniStream/run_q8.sh>> "$LOG_FILE" 2>&1

    TXTFILE="/tmp/flink_output.txt"
    if [ -f "$TXTFILE" ]; then
      cp /tmp/flink_output.txt ${OMNISTREAM_HOME}/testtool/queryfold/query8_OmniStream
            DEST_FOLDER="${QUERY_LOGS}/query8"
            mkdir -p "$DEST_FOLDER"

            cp /tmp/flink_output.txt "${DEST_FOLDER}/"
            cp ${OMNISTREAM_HOME}/testtool/queryfold/query8_flink/query8_result_flink.txt "${DEST_FOLDER}/"

    fi
    python3 ${OMNISTREAM_HOME}/testtool/queryfold/query8_OmniStream/q8logextractor.py
    python3 ${OMNISTREAM_HOME}/testtool/queryfold/query8_OmniStream/validate_result8.py
    FILE="${OMNISTREAM_HOME}/testtool/queryfold/query8_OmniStream/flink_output.txt"
    if [ -f "$FILE" ]; then
        rm "$FILE"
    fi
    echo "finish running query8 at time: $(date +"%H:%M:%S")"


        FILE2="${OMNISTREAM_HOME}/testtool/queryfold/query8_OmniStream/q8-taskname-OmniStream.txt"
        if [ -f "$FILE2" ]; then
            rm "$FILE2"
        fi

fi



if $run_all || [[ "$query_set" == *",9,"* ]]; then
    echo "start running query9 at time: $(date +"%H:%M:%S")"
    ${OMNISTREAM_HOME}/testtool/queryfold/query9_OmniStream/run_q9.sh>> "$LOG_FILE" 2>&1

    TXTFILE="/tmp/flink_output.txt"
    if [ -f "$TXTFILE" ]; then
      cp /tmp/flink_output.txt ${OMNISTREAM_HOME}/testtool/queryfold/query9_OmniStream

            DEST_FOLDER="${QUERY_LOGS}/query9"
            mkdir -p "$DEST_FOLDER"

            cp /tmp/flink_output.txt "${DEST_FOLDER}/"

            cp ${OMNISTREAM_HOME}/testtool/queryfold/query9_flink/query9_result_flink.txt "${DEST_FOLDER}/"


    fi
    python3 ${OMNISTREAM_HOME}/testtool/queryfold/query9_OmniStream/q9logextractor.py
    python3 ${OMNISTREAM_HOME}/testtool/queryfold/query9_OmniStream/validate_result9.py
    FILE="${OMNISTREAM_HOME}/testtool/queryfold/query9_OmniStream/flink_output.txt"
    if [ -f "$FILE" ]; then
        rm "$FILE"
    fi
    echo "finish running query9 at time: $(date +"%H:%M:%S")"

        FILE2="${OMNISTREAM_HOME}/testtool/queryfold/query9_OmniStream/q9-taskname-OmniStream.txt"
        if [ -f "$FILE2" ]; then
            rm "$FILE2"
        fi

fi


if $run_all || [[ "$query_set" == *",10,"* ]]; then
    echo "start running query10 at time: $(date +"%H:%M:%S")"
    ${OMNISTREAM_HOME}/testtool/queryfold/query10_OmniStream/run_q10.sh>> "$LOG_FILE" 2>&1
    TXTFILE="/tmp/flink_output.txt"
    if [ -f "$TXTFILE" ]; then
      cp /tmp/flink_output.txt ${OMNISTREAM_HOME}/testtool/queryfold/query10_OmniStream

            DEST_FOLDER="${QUERY_LOGS}/query10"
            mkdir -p "$DEST_FOLDER"

            cp /tmp/flink_output.txt "${DEST_FOLDER}/"
            cp ${OMNISTREAM_HOME}/testtool/queryfold/query10_flink/query10_result_flink.txt "${DEST_FOLDER}/"

    fi
    python3 ${OMNISTREAM_HOME}/testtool/queryfold/query10_OmniStream/q10logextractor.py
    python3 ${OMNISTREAM_HOME}/testtool/queryfold/query10_OmniStream/validate_result10.py
    FILE="${OMNISTREAM_HOME}/testtool/queryfold/query10_OmniStream/flink_output.txt"
    if [ -f "$FILE" ]; then
        rm "$FILE"
    fi
    echo "finish running query10 at time: $(date +"%H:%M:%S")"

        FILE2="${OMNISTREAM_HOME}/testtool/queryfold/query10_OmniStream/q10-taskname-OmniStream.txt"
        if [ -f "$FILE2" ]; then
            rm "$FILE2"
        fi

fi



if $run_all || [[ "$query_set" == *",11,"* ]]; then
    echo "start running query11 at time: $(date +"%H:%M:%S")"
    ${OMNISTREAM_HOME}/testtool/queryfold/query11_OmniStream/run_q11.sh>> "$LOG_FILE" 2>&1
    TXTFILE="/tmp/flink_output.txt"
    if [ -f "$TXTFILE" ]; then
      cp /tmp/flink_output.txt ${OMNISTREAM_HOME}/testtool/queryfold/query11_OmniStream
            DEST_FOLDER="${QUERY_LOGS}/query11"
            mkdir -p "$DEST_FOLDER"

            cp /tmp/flink_output.txt "${DEST_FOLDER}/"
            cp ${OMNISTREAM_HOME}/testtool/queryfold/query11_flink/query11_result_flink.txt "${DEST_FOLDER}/"

    fi
    python3 ${OMNISTREAM_HOME}/testtool/queryfold/query11_OmniStream/q11logextractor.py
    python3 ${OMNISTREAM_HOME}/testtool/queryfold/query11_OmniStream/validate_result11.py
    FILE="${OMNISTREAM_HOME}/testtool/queryfold/query11_OmniStream/flink_output.txt"
    if [ -f "$FILE" ]; then
        rm "$FILE"
    fi
    echo "finish running query11 at time: $(date +"%H:%M:%S")"
            FILE2="${OMNISTREAM_HOME}/testtool/queryfold/query11_OmniStream/q11-taskname-OmniStream.txt"
            if [ -f "$FILE2" ]; then
                rm "$FILE2"
            fi

fi






if $run_all || [[ "$query_set" == *",12,"* ]]; then
    echo "start running query12 at time: $(date +"%H:%M:%S")"
    ${OMNISTREAM_HOME}/testtool/queryfold/query12_OmniStream/run_q12.sh>> "$LOG_FILE" 2>&1
    TXTFILE="/tmp/flink_output.txt"
    if [ -f "$TXTFILE" ]; then
      cp /tmp/flink_output.txt ${OMNISTREAM_HOME}/testtool/queryfold/query12_OmniStream
            DEST_FOLDER="${QUERY_LOGS}/query12"
            mkdir -p "$DEST_FOLDER"

            cp /tmp/flink_output.txt "${DEST_FOLDER}/"
            cp ${OMNISTREAM_HOME}/testtool/queryfold/query12_flink/query12_result_flink.txt "${DEST_FOLDER}/"

    fi
    python3 ${OMNISTREAM_HOME}/testtool/queryfold/query12_OmniStream/q12logextractor.py
    python3 ${OMNISTREAM_HOME}/testtool/queryfold/query12_OmniStream/validate_result12.py
    FILE="${OMNISTREAM_HOME}/testtool/queryfold/query12_OmniStream/flink_output.txt"
    if [ -f "$FILE" ]; then
        rm "$FILE"
    fi
    echo "finish running query12 at time: $(date +"%H:%M:%S")"
    FILE2="${OMNISTREAM_HOME}/testtool/queryfold/query12_OmniStream/q12-taskname-OmniStream.txt"
            if [ -f "$FILE2" ]; then
                rm "$FILE2"
            fi

fi


if $run_all || [[ "$query_set" == *",13,"* ]]; then
    echo "start running query13 at time: $(date +"%H:%M:%S")"
    ${OMNISTREAM_HOME}/testtool/queryfold/query13_OmniStream/run_q13.sh>> "$LOG_FILE" 2>&1
    TXTFILE="/tmp/flink_output.txt"

    if [ -f "$TXTFILE" ]; then
      cp /tmp/flink_output.txt ${OMNISTREAM_HOME}/testtool/queryfold/query13_OmniStream
            DEST_FOLDER="${QUERY_LOGS}/query13"
            mkdir -p "$DEST_FOLDER"

            cp /tmp/flink_output.txt "${DEST_FOLDER}/"
            cp ${OMNISTREAM_HOME}/testtool/queryfold/query13_flink/query13_result_flink.txt "${DEST_FOLDER}/"

    fi
    python3 ${OMNISTREAM_HOME}/testtool/queryfold/query13_OmniStream/q13logextractor.py
    python3 ${OMNISTREAM_HOME}/testtool/queryfold/query13_OmniStream/validate_result13.py
    FILE="${OMNISTREAM_HOME}/testtool/queryfold/query13_OmniStream/flink_output.txt"
    if [ -f "$FILE" ]; then
        rm "$FILE"
    fi
    echo "finish running query13 at time: $(date +"%H:%M:%S")"
            FILE2="${OMNISTREAM_HOME}/testtool/queryfold/query13_OmniStream/q13-taskname-OmniStream.txt"
            if [ -f "$FILE2" ]; then
                rm "$FILE2"
            fi

fi

if $run_all || [[ "$query_set" == *",14,"* ]]; then
    echo "start running query14 at time: $(date +"%H:%M:%S")"
    ${OMNISTREAM_HOME}/testtool/queryfold/query14_OmniStream/run_q14.sh>> "$LOG_FILE" 2>&1
    TXTFILE="/tmp/flink_output.txt"
    if [ -f "$TXTFILE" ]; then
      cp /tmp/flink_output.txt ${OMNISTREAM_HOME}/testtool/queryfold/query14_OmniStream
            DEST_FOLDER="${QUERY_LOGS}/query14"
            mkdir -p "$DEST_FOLDER"

            cp /tmp/flink_output.txt "${DEST_FOLDER}/"


            cp ${OMNISTREAM_HOME}/testtool/queryfold/query14_flink/query14_result_flink.txt "${DEST_FOLDER}/"


    fi
    python3 ${OMNISTREAM_HOME}/testtool/queryfold/query14_OmniStream/q14logextractor.py
    python3 ${OMNISTREAM_HOME}/testtool/queryfold/query14_OmniStream/validate_result14.py
    FILE="${OMNISTREAM_HOME}/testtool/queryfold/query14_OmniStream/flink_output.txt"
    if [ -f "$FILE" ]; then
        rm "$FILE"
    fi
    echo "finish running query14 at time: $(date +"%H:%M:%S")"
            FILE2="${OMNISTREAM_HOME}/testtool/queryfold/query14_OmniStream/q14-taskname-OmniStream.txt"
            if [ -f "$FILE2" ]; then
                rm "$FILE2"
            fi

fi


if $run_all || [[ "$query_set" == *",15,"* ]]; then
    echo "start running query15 at time: $(date +"%H:%M:%S")"
    ${OMNISTREAM_HOME}/testtool/queryfold/query15_OmniStream/run_q15.sh ${OMNISTREAM_HOME}/testtool/queryfold/query15_OmniStream/query15_filled.sql>> "$LOG_FILE" 2>&1
#    ${OMNISTREAM_HOME}/testtool/queryfold/query15_OmniStream/run_q15.sh
    TXTFILE="/tmp/flink_output.txt"
    if [ -f "$TXTFILE" ]; then
      cp /tmp/flink_output.txt ${OMNISTREAM_HOME}/testtool/queryfold/query15_OmniStream
            DEST_FOLDER="${QUERY_LOGS}/query15"
            mkdir -p "$DEST_FOLDER"

            cp /tmp/flink_output.txt "${DEST_FOLDER}/"
            cp ${OMNISTREAM_HOME}/testtool/queryfold/query15_flink/query15_result_flink.txt "${DEST_FOLDER}/"

    fi
    python3 ${OMNISTREAM_HOME}/testtool/queryfold/query15_OmniStream/q15logextractor.py
    python3 ${OMNISTREAM_HOME}/testtool/queryfold/query15_OmniStream/validate_result15.py
    FILE="${OMNISTREAM_HOME}/testtool/queryfold/query15_OmniStream/flink_output.txt"
    if [ -f "$FILE" ]; then
        rm "$FILE"
    fi
    echo "finish running query15 at time: $(date +"%H:%M:%S")"
            FILE2="${OMNISTREAM_HOME}/testtool/queryfold/query15_OmniStream/q15-taskname-OmniStream.txt"
            if [ -f "$FILE2" ]; then
                rm "$FILE2"
            fi

fi

if $run_all || [[ "$query_set" == *",16,"* ]]; then
    echo "start running query16 at time: $(date +"%H:%M:%S")"
    ${OMNISTREAM_HOME}/testtool/queryfold/query16_OmniStream/run_q16.sh ${OMNISTREAM_HOME}/testtool/queryfold/query16_OmniStream/query16_filled.sql>> "$LOG_FILE" 2>&1

    TXTFILE="/tmp/flink_output.txt"
    if [ -f "$TXTFILE" ]; then
      cp /tmp/flink_output.txt ${OMNISTREAM_HOME}/testtool/queryfold/query16_OmniStream
            DEST_FOLDER="${QUERY_LOGS}/query16"
            mkdir -p "$DEST_FOLDER"

            cp /tmp/flink_output.txt "${DEST_FOLDER}/"
            cp ${OMNISTREAM_HOME}/testtool/queryfold/query16_flink/query16_result_flink.txt "${DEST_FOLDER}/"

    fi
    python3 ${OMNISTREAM_HOME}/testtool/queryfold/query16_OmniStream/q16logextractor.py
    python3 ${OMNISTREAM_HOME}/testtool/queryfold/query16_OmniStream/validate_result16.py
    FILE="${OMNISTREAM_HOME}/testtool/queryfold/query16_OmniStream/flink_output.txt"
    if [ -f "$FILE" ]; then
        rm "$FILE"
    fi
    echo "finish running query16 at time: $(date +"%H:%M:%S")"
            FILE2="${OMNISTREAM_HOME}/testtool/queryfold/query16_OmniStream/q16-taskname-OmniStream.txt"
            if [ -f "$FILE2" ]; then
                rm "$FILE2"
            fi

fi



if $run_all || [[ "$query_set" == *",17,"* ]]; then
    echo "start running query17 at time: $(date +"%H:%M:%S")"
    ${OMNISTREAM_HOME}/testtool/queryfold/query17_OmniStream/run_q17.sh ${OMNISTREAM_HOME}/testtool/queryfold/query17_OmniStream/query17_filled.sql>> "$LOG_FILE" 2>&1
    TXTFILE="/tmp/flink_output.txt"
    if [ -f "$TXTFILE" ]; then
      cp /tmp/flink_output.txt ${OMNISTREAM_HOME}/testtool/queryfold/query17_OmniStream
            DEST_FOLDER="${QUERY_LOGS}/query17"
            mkdir -p "$DEST_FOLDER"

            cp /tmp/flink_output.txt "${DEST_FOLDER}/"
            cp ${OMNISTREAM_HOME}/testtool/queryfold/query17_flink/query17_result_flink.txt "${DEST_FOLDER}/"

    fi
    python3 ${OMNISTREAM_HOME}/testtool/queryfold/query17_OmniStream/q17logextractor.py
    python3 ${OMNISTREAM_HOME}/testtool/queryfold/query17_OmniStream/validate_result17.py
    FILE="${OMNISTREAM_HOME}/testtool/queryfold/query17_OmniStream/flink_output.txt"
    if [ -f "$FILE" ]; then
        rm "$FILE"
    fi
    echo "finish running query17 at time: $(date +"%H:%M:%S")"
            FILE2="${OMNISTREAM_HOME}/testtool/queryfold/query17_OmniStream/q17-taskname-OmniStream.txt"
            if [ -f "$FILE2" ]; then
                rm "$FILE2"
            fi

fi

if $run_all || [[ "$query_set" == *",18,"* ]]; then
    echo "start running query18 at time: $(date +"%H:%M:%S")"
    ${OMNISTREAM_HOME}/testtool/queryfold/query18_OmniStream/run_q18.sh>> "$LOG_FILE" 2>&1
    TXTFILE="/tmp/flink_output.txt"
    if [ -f "$TXTFILE" ]; then
      cp /tmp/flink_output.txt ${OMNISTREAM_HOME}/testtool/queryfold/query18_OmniStream
            DEST_FOLDER="${QUERY_LOGS}/query18"
            mkdir -p "$DEST_FOLDER"

            cp /tmp/flink_output.txt "${DEST_FOLDER}/"



            cp ${OMNISTREAM_HOME}/testtool/queryfold/query18_flink/query18_result_flink.txt "${DEST_FOLDER}/"


    fi
    python3 ${OMNISTREAM_HOME}/testtool/queryfold/query18_OmniStream/q18logextractor.py
    python3 ${OMNISTREAM_HOME}/testtool/queryfold/query18_OmniStream/validate_result18.py
    FILE="${OMNISTREAM_HOME}/testtool/queryfold/query18_OmniStream/flink_output.txt"
    if [ -f "$FILE" ]; then
        rm "$FILE"
    fi
    echo "finish running query18 at time: $(date +"%H:%M:%S")"
            FILE2="${OMNISTREAM_HOME}/testtool/queryfold/query18_OmniStream/q18-taskname-OmniStream.txt"
            if [ -f "$FILE2" ]; then
                rm "$FILE2"
            fi

fi

if $run_all || [[ "$query_set" == *",19,"* ]]; then
    echo "start running query19 at time: $(date +"%H:%M:%S")"
    ${OMNISTREAM_HOME}/testtool/queryfold/query19_OmniStream/run_q19.sh>> "$LOG_FILE" 2>&1
    TXTFILE="/tmp/flink_output.txt"
    if [ -f "$TXTFILE" ]; then
      cp /tmp/flink_output.txt ${OMNISTREAM_HOME}/testtool/queryfold/query19_OmniStream
            DEST_FOLDER="${QUERY_LOGS}/query19"
            mkdir -p "$DEST_FOLDER"

            cp /tmp/flink_output.txt "${DEST_FOLDER}/"


            cp ${OMNISTREAM_HOME}/testtool/queryfold/query19_flink/query19_result_flink.txt "${DEST_FOLDER}/"


    fi
    python3 ${OMNISTREAM_HOME}/testtool/queryfold/query19_OmniStream/q19logextractor.py
    python3 ${OMNISTREAM_HOME}/testtool/queryfold/query19_OmniStream/validate_result19.py
    FILE="${OMNISTREAM_HOME}/testtool/queryfold/query19_OmniStream/flink_output.txt"
    if [ -f "$FILE" ]; then
        rm "$FILE"
    fi
    echo "finish running query19 at time: $(date +"%H:%M:%S")"
            FILE2="${OMNISTREAM_HOME}/testtool/queryfold/query19_OmniStream/q19_taskname-OmniStream.txt"
            if [ -f "$FILE2" ]; then
                rm "$FILE2"
            fi

fi


if $run_all || [[ "$query_set" == *",20,"* ]]; then
    echo "start running query20 at time: $(date +"%H:%M:%S")"
    ${OMNISTREAM_HOME}/testtool/queryfold/query20_OmniStream/run_q20.sh>> "$LOG_FILE" 2>&1

    TXTFILE="/tmp/flink_output.txt"
    if [ -f "$TXTFILE" ]; then
      cp /tmp/flink_output.txt ${OMNISTREAM_HOME}/testtool/queryfold/query20_OmniStream

            DEST_FOLDER="${QUERY_LOGS}/query20"
            mkdir -p "$DEST_FOLDER"

            cp /tmp/flink_output.txt "${DEST_FOLDER}/"
            cp ${OMNISTREAM_HOME}/testtool/queryfold/query20_flink/query20_result_flink.txt "${DEST_FOLDER}/"

    fi
    python3 ${OMNISTREAM_HOME}/testtool/queryfold/query20_OmniStream/q20logextractor.py
    python3 ${OMNISTREAM_HOME}/testtool/queryfold/query20_OmniStream/validate_result20.py
    FILE="${OMNISTREAM_HOME}/testtool/queryfold/query20_OmniStream/flink_output.txt"
    if [ -f "$FILE" ]; then
        rm "$FILE"
    fi
    echo "finish running query20 at time: $(date +"%H:%M:%S")"
            FILE2="${OMNISTREAM_HOME}/testtool/queryfold/query20_OmniStream/q20-taskname-OmniStream.txt"
            if [ -f "$FILE2" ]; then
                rm "$FILE2"
            fi

fi


if $run_all || [[ "$query_set" == *",21,"* ]]; then
    echo "start running query21 at time: $current_time"
    ${OMNISTREAM_HOME}/testtool/queryfold/query21_OmniStream/run_q21.sh ${OMNISTREAM_HOME}/testtool/queryfold/query21_OmniStream/query21_filled.sql>> "$LOG_FILE" 2>&1
    TXTFILE="/tmp/flink_output.txt"
    if [ -f "$TXTFILE" ]; then
      cp /tmp/flink_output.txt ${OMNISTREAM_HOME}/testtool/queryfold/query21_OmniStream
            DEST_FOLDER="${QUERY_LOGS}/query21"
            mkdir -p "$DEST_FOLDER"

            cp /tmp/flink_output.txt "${DEST_FOLDER}/"


            cp ${OMNISTREAM_HOME}/testtool/queryfold/query21_flink/query21_result_flink.txt "${DEST_FOLDER}/"


    fi
    python3 ${OMNISTREAM_HOME}/testtool/queryfold/query21_OmniStream/q21logextractor.py
    python3 ${OMNISTREAM_HOME}/testtool/queryfold/query21_OmniStream/validate_21.py
    FILE="${OMNISTREAM_HOME}/testtool/queryfold/query21_OmniStream/flink_output.txt"
    if [ -f "$FILE" ]; then
        rm "$FILE"
    fi
    echo "finish running query21 at time: $(date +"%H:%M:%S")"
            FILE2="${OMNISTREAM_HOME}/testtool/queryfold/query21_OmniStream/q21-taskname-OmniStream.txt"
            if [ -f "$FILE2" ]; then
                rm "$FILE2"
            fi

fi


if $run_all || [[ "$query_set" == *",22,"* ]]; then
    echo "start running query22 at time: $(date +"%H:%M:%S")"
    ${OMNISTREAM_HOME}/testtool/queryfold/query22_OmniStream/run_q22.sh>> "$LOG_FILE" 2>&1
    TXTFILE="/tmp/flink_output.txt"
    if [ -f "$TXTFILE" ]; then
      cp /tmp/flink_output.txt ${OMNISTREAM_HOME}/testtool/queryfold/query22_OmniStream

            DEST_FOLDER="${QUERY_LOGS}/query22"
            mkdir -p "$DEST_FOLDER"

            cp /tmp/flink_output.txt "${DEST_FOLDER}/"

            cp ${OMNISTREAM_HOME}/testtool/queryfold/query22_flink/query22_result_flink.txt "${DEST_FOLDER}/"


    fi
    python3 ${OMNISTREAM_HOME}/testtool/queryfold/query22_OmniStream/q22logextractor.py
    python3 ${OMNISTREAM_HOME}/testtool/queryfold/query22_OmniStream/validate_22.py
    FILE="${OMNISTREAM_HOME}/testtool/queryfold/query22_OmniStream/flink_output.txt"
    if [ -f "$FILE" ]; then
        rm "$FILE"
    fi
    echo "finish running query22 at time: $(date +"%H:%M:%S")"
            FILE2="${OMNISTREAM_HOME}/testtool/queryfold/query22_OmniStream/q22-taskname-OmniStream.txt"
            if [ -f "$FILE2" ]; then
                rm "$FILE2"
            fi

fi




cd "${OMNISTREAM_HOME}/testtool/entrypoint"

#if ls hs_err* 1> /dev/null 2>&1; then
#    rm -f hs_err*
#fi




