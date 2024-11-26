# IMPORTANT: When sourcing, make sure the pwd is this file's location!
# i.e. PWD=..../etl-scripts/

# shorthand for echo to stderr, which is done all over the place in many scripts
# Used for sending messages to the developer but not messages that other programs should be reading, i.e. reading from stdout
er() {
    echo "$*" >&2
}

error_exit() {
    er Error: "$*"
    exit 1
}

if [[ ! -f .env ]]; then
    error_exit No .env found for etl-scripts repo
fi

source .env

# Validate that the .env is set correctly
if [[ -z "$WEBHOOK_URL" ]]; then
    error_exit Empty WEBHOOK_URL in .env
fi

send_message_to_webhook() {
    # joins all args with first character of IFS bash variable, usually space
    message="$*"

    unix_timestamp=$(date -u +%s)
    # Format the previous timestamp rather than getting a new one with date, to avoid issues time changing in between
    date_format='%R:%S %a %d %b %Y UTC'
    timestamp=$(date -u --date="@$unix_timestamp" +"$date_format")

    message_content="[$(hostname)] [$unix_timestamp $timestamp] $message"

    curl -H "Content-Type: application/json" -X POST -d "{\"content\": \"$message_content\"}" $WEBHOOK_URL
}

# Echo and send to Webhook
echo_and_webhook() {
    message="$*"
    er "$message"
    send_message_to_webhook "$message"
}
