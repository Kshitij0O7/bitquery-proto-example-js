const { Kafka } = require('kafkajs');
const protobuf = require('protobufjs');
const bs58 = require('bs58');
// const path = require('path');
const { CompressionTypes, CompressionCodecs } = require("kafkajs");
const LZ4 = require("kafkajs-lz4");
require('dotenv').config();

CompressionCodecs[CompressionTypes.LZ4] = new LZ4().codec;

let ParsedIdlBlockMessage;
const username = process.env.USERNAME;
const password = process.env.PASSWORD;
const topic = 'bsc.dextrades.proto';

const loadProto = async () => {
    const root = await protobuf.load('proto/evm/dex_block_message.proto');
    ParsedIdlBlockMessage = root.lookupType('evm_messages.DexBlockMessage');
};

const convertBytes = (buffer, encoding = 'base58') => {
    if (encoding === 'base58') {
        return bs58.default.encode(buffer);
    }
    return buffer.toString('hex');
}

const printProtobufMessage = (msg, indent = 0, encoding = 'base58') => {
    const prefix = ' '.repeat(indent);
    for (const [key, value] of Object.entries(msg)) {
        if (Array.isArray(value)) {
            console.log(`${prefix}${key} (repeated):`);
            value.forEach((item, idx) => {
                if (typeof item === 'object' && item !== null) {
                    console.log(`${prefix}  [${idx}]:`);
                    printProtobufMessage(item, indent + 4, encoding);
                } else {
                    console.log(`${prefix}  [${idx}]: ${item}`);
                }
            });
        } else if (value && typeof value === 'object' && Buffer.isBuffer(value)) {
            console.log(`${prefix}${key}: ${convertBytes(value, encoding)}`);
        } else if (value && typeof value === 'object') {
            console.log(`${prefix}${key}:`);
            printProtobufMessage(value, indent + 4, encoding);
        } else {
            console.log(`${prefix}${key}: ${value}`);
        }
    }
}

const kafka = new Kafka({
    clientId: username,
    brokers: ['rpk0.bitquery.io:9092', 'rpk1.bitquery.io:9092', 'rpk2.bitquery.io:9092'],
    sasl: {
        mechanism: "scram-sha-512",
        username: username,
        password: password
    }
});

const consumer = kafka.consumer({ groupId: username + '-group123' });

async function run() {
    await loadProto(); // Load proto before starting Kafka
    await consumer.connect();
    await consumer.subscribe({ topic, fromBeginning: false });

    await consumer.run({
        autoCommit: false,
        eachMessage: async ({ partition, message }) => {
            try {
                const buffer = message.value;
                const decoded = ParsedIdlBlockMessage.decode(buffer);
                const msgObj = ParsedIdlBlockMessage.toObject(decoded, { bytes: Buffer });
                console.log("\nNew Protobuf Message Received:\n");
                printProtobufMessage(msgObj);
            } catch (err) {
                console.error('Error decoding Protobuf message:', err);
            }
        },
    });
}

run().catch(console.error);