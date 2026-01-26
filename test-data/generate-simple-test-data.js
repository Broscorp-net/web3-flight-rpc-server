const { ethers } = require('ethers');

async function main() {
    const nodeUrl = process.env.ETH_NODE_URL || 'http://localhost:8545';
    console.log(`Connecting to Ethereum node at ${nodeUrl}...`);

    const provider = new ethers.JsonRpcProvider(nodeUrl);

    // Wait for node to be ready
    let ready = false;
    for (let i = 0; i < 30; i++) {
        try {
            await provider.getBlockNumber();
            ready = true;
            console.log('Node is ready!');
            break;
        } catch (e) {
            console.log(`Waiting for node... attempt ${i + 1}/30`);
            await new Promise(resolve => setTimeout(resolve, 2000));
        }
    }

    if (!ready) {
        console.error('Node did not become ready in time');
        process.exit(1);
    }

    // Get signers from Hardhat (Hardhat provides 20 pre-funded accounts)
    const accounts = await provider.listAccounts();
    console.log(`Found ${accounts.length} accounts`);

    const signer1 = await provider.getSigner(0);
    const signer2 = await provider.getSigner(1);
    const signer3 = await provider.getSigner(2);

    const addr1 = await signer1.getAddress();
    const addr2 = await signer2.getAddress();
    const addr3 = await signer3.getAddress();

    console.log('\n=== Generating Simple Test Transactions ===');
    console.log('These are simple ETH transfers that will create blocks and transaction data');

    // Generate 50 simple ETH transfer transactions
    for (let i = 0; i < 50; i++) {
        const amount = ethers.parseEther((Math.random() * 0.1).toFixed(6));

        try {
            let tx;
            if (i % 3 === 0) {
                // Transfer from signer1 to signer2
                tx = await signer1.sendTransaction({
                    to: addr2,
                    value: amount
                });
                console.log(`[${i + 1}/50] Transfer: ${addr1.slice(0, 10)}... -> ${addr2.slice(0, 10)}..., ${ethers.formatEther(amount)} ETH (tx: ${tx.hash.slice(0, 10)}...)`);
            } else if (i % 3 === 1) {
                // Transfer from signer2 to signer3
                tx = await signer2.sendTransaction({
                    to: addr3,
                    value: amount
                });
                console.log(`[${i + 1}/50] Transfer: ${addr2.slice(0, 10)}... -> ${addr3.slice(0, 10)}..., ${ethers.formatEther(amount)} ETH (tx: ${tx.hash.slice(0, 10)}...)`);
            } else {
                // Transfer from signer3 to signer1
                tx = await signer3.sendTransaction({
                    to: addr1,
                    value: amount
                });
                console.log(`[${i + 1}/50] Transfer: ${addr3.slice(0, 10)}... -> ${addr1.slice(0, 10)}..., ${ethers.formatEther(amount)} ETH (tx: ${tx.hash.slice(0, 10)}...)`);
            }

            await tx.wait();

        } catch (error) {
            console.error(`Error on transaction ${i + 1}:`, error.message);
        }

        // Small delay every 10 transactions
        if (i > 0 && i % 10 === 0) {
            await new Promise(resolve => setTimeout(resolve, 100));
        }
    }

    const currentBlock = await provider.getBlockNumber();
    console.log(`\n=== Test Data Generation Complete ===`);
    console.log(`Current block number: ${currentBlock}`);
    console.log(`Total transactions: 50`);
    console.log(`\nYou can now query the Flight server for blocks!`);
    console.log(`\nExample queries:`);
    console.log(`- All blocks: {"dataset": "blocks", "startBlock": "0", "endBlock": "${currentBlock}"}`);
    console.log(`- Recent blocks: {"dataset": "blocks", "startBlock": "${Math.max(0, currentBlock - 10)}", "endBlock": "${currentBlock}"}`);
    console.log(`\nNote: These are simple transfers without contract events, so there will be no logs.`);
    console.log(`For log data, you would need to deploy and interact with smart contracts.`);
}

main()
    .then(() => {
        console.log('\nTest data generation successful!');
        process.exit(0);
    })
    .catch(error => {
        console.error('Error:', error);
        process.exit(1);
    });
