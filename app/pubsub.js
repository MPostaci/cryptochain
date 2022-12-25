const redis = require('redis');

const CHANNELS = {
    TEST: 'TEST',
    BLOCKCHAIN: 'BLOCKCHAIN',
    TRANSACTION: 'TRANSACTION'
};

class PubSub {
    constructor({blockchain, transactionPool}){
        this.blockchain = blockchain;
        this.transactionPool = transactionPool;

        this.publisher = redis.createClient({url: 'redis://default:rsLMIkbaeNawdGdA37Fk1t18Rkn9wR1u@redis-16354.c135.eu-central-1-1.ec2.cloud.redislabs.com:16354'});
        this.subscriber = redis.createClient({url: 'redis://default:rsLMIkbaeNawdGdA37Fk1t18Rkn9wR1u@redis-16354.c135.eu-central-1-1.ec2.cloud.redislabs.com:16354'});

        this.subscribeToChannels();

        this.subscriber.on('message', (channel, message) => this.handleMessage(channel, message)) ;
    }

    handleMessage(channel, message) {
        console.log(`Message received. Channel: ${channel}. Message: ${message}.`);

        const parsedMessage = JSON.parse(message);

        switch(channel) {
            case CHANNELS.BLOCKCHAIN:
                this.blockchain.replaceChain(parsedMessage, true, () => {
                    this.transactionPool.clearBlockchainTransactions({
                        chain: parsedMessage
                    });
                });
                break;
            case CHANNELS.TRANSACTION:
                this.transactionPool.setTransaction(parsedMessage);
                break;
            default:
                return;
        }
    }

    subscribeToChannels() {
        Object.values(CHANNELS).forEach((channel) => {
            this.subscriber.subscribe(channel); 
        });
    }

    publish({channel, message}){
        this.subscriber.unsubscribe(channel, () => {
            this.publisher.publish(channel, message, () => {
                this.subscriber.subscribe(channel);
            });
        });
    }

    broadcastChain() {
        this.publish({
            channel: CHANNELS.BLOCKCHAIN,
            message: JSON.stringify(this.blockchain.chain)
        })
    }

    broadcastTransaction(transaction){
        this.publish({
            channel: CHANNELS.TRANSACTION,
            message: JSON.stringify(transaction)
        })
    }
}

module.exports = PubSub;