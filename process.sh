#!/bin/bash

# Required environment variables:
#
# QC_DATAFLEET_DEPLOYMENT_DATE (date formatted as "%Y%m%d")
# FTP_HOST
# FTP_USERNAME
# FTP_PASSWORD
# S3_BUCKET_NAME

# Optional environment variables:
# 
# S3_OPTIONS (CLI arguments to AWS CLI)
# PROCESS_HISTORICAL_DATA (if equal to "true", then historical data will be downloaded and processed)

S3_OPTIONS="${S3_OPTIONS:-}"
TRUEBEATS_FILE_PREFIXES=( "ExtractAlpha_All_TrueBeats_EPS_US" "ExtractAlpha_All_TrueBeats_SALES_US" "ExtractAlpha_FQ1_TrueBeats_EPS_US" "ExtractAlpha_FQ1_TrueBeats_SALES_US" "Fiscal_Periods_EPSSales_US" )


function sync_to_s3 {
    echo "Begin syncing data to S3 for date: ${QC_DATAFLEET_DEPLOYMENT_DATE}"
    
    for file_prefix in ${TRUEBEATS_FILE_PREFIXES[@]}; do
        file_name="${file_prefix}_${QC_DATAFLEET_DEPLOYMENT_DATE}.csv"
        
        aws s3 cp ${file_name} ${S3_OPTIONS} s3://${S3_BUCKET_NAME}/alternative/extractalpha/truebeats/
        exit_code=$?
        
        if [ "${exit_code}" -ne 0 ]; then
            echo "Download failed for file: ${file_name} - Exiting with code ${exit_code}"
            return ${exit_code}
        fi;
        
        echo "Successfully completed sync to S3 for file: ${file_name}"
    done;
    
    echo "Successfully completed S3 sync for date: ${QC_DATAFLEET_DEPLOYMENT_DATE}"
}

function get_historical_raw_from_s3 {
    echo "Begin downloading historical raw data from archive S3 bucket"
    aws s3 cp ${S3_OPTIONS} s3://${S3_BUCKET_NAME}/alternative/extractalpha/truebeats/ExtractAlpha_TrueBeats_EPSSALES_History_US.zip ./
    exit_code=$?
    
    if [ "${exit_code}" -ne 0 ]; then
        echo "Historical raw data download from S3 bucket ${S3_BUCKET_NAME} failed. Exiting with code ${exit_code}"
        exit ${exit_code}
    fi;
    
    echo "Unzipping historical raw data..."
    unzip ExtractAlpha_TrueBeats_EPSSALES_History_US.zip
    
    exit_code=$?
    if [ "${exit_code}" -ne 0 ]; then
        echo "Failed to unzip historical raw data. Exiting with code ${exit_code}"
        exit ${exit_code}
    fi;
    
    echo "Successfully retrieved historical raw data"
}

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
    exit_code=$?
    if [ "${exit_code}" -ne 0 ]; then
        echo "Download from FTP host ${FTP_HOST} failed. Exiting with code ${exit_code}"
        exit ${exit_code}
    fi;
    
    echo "Successfully finished downloading data from the FTP for date: ${QC_DATAFLEET_DEPLOYMENT_DATE}"
    sync_to_s3
}

if [[ "${PROCESS_HISTORICAL_DATA}" == "true" ]]; then
    get_historical_raw_from_s3
    exit $?
fi;

ftp_download_truebeats