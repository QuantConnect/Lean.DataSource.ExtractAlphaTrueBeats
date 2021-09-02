#!/bin/bash

# Required environment variables:
#
# QC_DATAFLEET_DEPLOYMENT_DATE (date formatted as "%Y%m%d")
# FTP_HOST
# FTP_USERNAME
# FTP_PASSWORD

TRUEBEATS_FILE_PREFIXES=( "ExtractAlpha_All_TrueBeats_EPS_US" "ExtractAlpha_All_TrueBeats_SALES_US" "ExtractAlpha_FQ1_TrueBeats_EPS_US" "ExtractAlpha_FQ1_TrueBeats_SALES_US" "Fiscal_Periods_EPSSales_US" )


function ftp_download_truebeats {
    for file_prefix in ${TRUEBEATS_FILE_PREFIXES[@]}; do
        new_command="get ${file_prefix}_${QC_DATAFLEET_DEPLOYMENT_DATE}.csv"

        if [ -z "${download_command}" ]; then
            download_command="${new_command}"
        else
            download_command="${download_command}; ${new_command}"
        fi;
    done;

    lftp -u "${FTP_USERNAME}":"${FTP_PASSWORD}" "ftp://${FTP_HOST}" -e "${download_command}; exit"
}

ftp_download_truebeats