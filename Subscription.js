import PubSubApiClient from 'salesforce-pubsub-api-client';

async function run() {
    try {
        const client = new PubSubApiClient(console);
        //await client.connect();
        await client.connectWithAuth(process.env.SALESFORCE_ACCESSTOKEN, process.env.SALESFORCE_LOGIN_URL);

        // Subscribe to account change events

        const eventEmitter = await client.subscribe(
            process.env.SALESFORCE_EVENT, 1
        );

        // Handle incoming events
        eventEmitter.on('data', (event) => {
            console.log(
                `Handling ${event.payload.ChangeEventHeader.entityName} change event ` +
                    `with ID ${event.replayId} ` +
                    `on channel ${eventEmitter.getTopicName()} ` +
                    `(${eventEmitter.getReceivedEventCount()}/${eventEmitter.getRequestedEventCount()} ` +
                    `events received so far)`
            );
            // Safely log event as a JSON string
            console.log( 'formatted data:'+
                JSON.stringify(
                    event,
                    (key, value) =>
                        /* Convert BigInt values into strings and keep other types unchanged */
                        typeof value === 'bigint'
                            ? value.toString()
                            : value,
                    2
                )
            );
        });
        eventEmitter.on('lastevent', (event) => {
            console.log( 'lastevent:'+
                JSON.stringify( event));
         } );
    } catch (error) {
        console.error(error);
    }
}

run();