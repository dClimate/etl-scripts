# For this script to be called correctly, it needs to be called when its current working directory is the repo folder, i.e. PWD=..../etl-scripts/

error_exit() {
    echo "Error: $1" >&2
    exit 1
}

if [[ ! -f .env ]]; then
    error_exit "No .env found for CPC"
fi

source ./.env

if [[ -z "$WEBHOOK_URL" ]]; then
    error_exit "Empty WEBHOOK_URL"
fi
if [[ -z "$NODE_IDENTIFIER" ]]; then
    error_exit "Empty NODE_IDENTIFIER"
fi

send_message_to_webhook() {
    message="$1"

    # Don't branch here since this usage of date is bsd and gnu compatible
    unix_timestamp=$(date -u +%s)
    # Format the previous timestamp rather than getting a new one with date, to avoid issues with a second rolling over on calling the date command a second time
    # Branch on whether we are on mac, during development 'date' kept pointing to BSD date rather than the aliased gdate from GNU coreutils so we just branch instead since --date is a platform dependent option
    date_format='%R:%S %a %d %b %Y UTC'
    case "$OSTYPE" in
        darwin*)
            timestamp=$(gdate -u --date="@$unix_timestamp" +"$date_format")
            ;;
        linux-gnu)
            timestamp=$(date -u --date="@$unix_timestamp" +"$date_format")
            ;;
        *)
            error_exit "Unsupported OSTYPE"
            ;;
    esac

    message_content="[$unix_timestamp $timestamp] [Node $NODE_IDENTIFIER] $message"

    curl -H "Content-Type: application/json" -X POST -d "{\"content\": \"\`$message_content\`\"}" $WEBHOOK_URL
}

echo_and_webhook() {
    echo $1 >&2

    # This needs quotes around $1 otherwise it will only pass the first word in
    send_message_to_webhook "$1"
}

check_there_is_one_argument() {
    num_arguments=$1
    if (( num_arguments != 1 )); then
        error_usage_exit "Invalid number of arguments, expected 1 and received $num_arguments"
    fi
}
