const { ethers } = require('ethers');
const solc = require('solc');

// Solidity contract source code
const contractSource = `
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

contract EventEmitter {
    event Transfer(address indexed from, address indexed to, uint256 value);
    event Approval(address indexed owner, address indexed spender, uint256 value);

    function emitTransfer(address to, uint256 amount) external {
        emit Transfer(msg.sender, to, amount);
    }

    function emitApproval(address spender, uint256 amount) external {
        emit Approval(msg.sender, spender, amount);
    }
}
`;

function compileContract() {
    console.log('Compiling contract with Solidity 0.8.19...');

    const input = {
        language: 'Solidity',
        sources: {
            'EventEmitter.sol': {
                content: contractSource
            }
        },
        settings: {
            optimizer: {
                enabled: true,
                runs: 200
            },
            evmVersion: 'london', // Compatible with most EVM versions
            outputSelection: {
                '*': {
                    '*': ['abi', 'evm.bytecode']
                }
            }
        }
    };

    const output = JSON.parse(solc.compile(JSON.stringify(input)));

    if (output.errors) {
        output.errors.forEach(err => {
            console.error(err.formattedMessage);
        });
        if (output.errors.some(err => err.severity === 'error')) {
            throw new Error('Compilation failed');
        }
    }

    const contract = output.contracts['EventEmitter.sol']['EventEmitter'];
    console.log('✓ Contract compiled successfully');
    console.log(`  ABI entries: ${contract.abi.length}`);

    return {
        abi: contract.abi,
        bytecode: '0x' + contract.evm.bytecode.object
    };
}

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

    // Get signers from Hardhat
    const signers = await provider.listAccounts();
    console.log(`Found ${signers.length} accounts`);

    const deployer = await provider.getSigner(0);
    const user1 = await provider.getSigner(1);
    const user2 = await provider.getSigner(2);

    // Compile the contract
    const compiled = compileContract();
    console.log(`Bytecode length: ${compiled.bytecode.length} characters`);

    console.log('\n=== Deploying Event Emitter Contract ===');
    let token, tokenAddress;
    try {
        const factory = new ethers.ContractFactory(compiled.abi, compiled.bytecode, deployer);
        console.log('Factory created, deploying contract...');
        token = await factory.deploy();
        console.log('Deployment transaction sent, waiting for confirmation...');
        await token.waitForDeployment();
        tokenAddress = await token.getAddress();
        console.log(`✓ Contract deployed at: ${tokenAddress}`);

        // Verify deployment
        const code = await provider.getCode(tokenAddress);
        console.log(`Contract code size: ${code.length} bytes`);
        if (code === '0x') {
            console.error('ERROR: Contract not deployed properly (no bytecode at address)');
            process.exit(1);
        }
    } catch (error) {
        console.error('Deployment failed:', error.message);
        if (error.data) {
            console.error('Error data:', error.data);
        }
        if (error.transaction) {
            console.error('Transaction:', error.transaction);
        }
        process.exit(1);
    }

    console.log('\n=== Generating Test Transactions and Logs ===');

    const user1Addr = await user1.getAddress();
    const user2Addr = await user2.getAddress();
    let successCount = 0;

    // Generate 50 transactions with events
    for (let i = 0; i < 50; i++) {
        const amount = ethers.parseEther((Math.random() * 100).toFixed(4));

        try {
            let tx, receipt;
            if (i % 2 === 0) {
                // Emit Transfer event
                tx = await token.connect(deployer).emitTransfer(user1Addr, amount);
                receipt = await tx.wait();
                console.log(`[${i + 1}/50] Transfer: deployer -> user1, amount: ${ethers.formatEther(amount)} TEST (tx: ${tx.hash.slice(0, 10)}..., logs: ${receipt.logs.length})`);
            } else {
                // Emit Approval event
                tx = await token.connect(deployer).emitApproval(user1Addr, amount);
                receipt = await tx.wait();
                console.log(`[${i + 1}/50] Approval: deployer -> user1, amount: ${ethers.formatEther(amount)} TEST (tx: ${tx.hash.slice(0, 10)}..., logs: ${receipt.logs.length})`);
            }

            if (receipt.logs.length === 0 && i === 0) {
                console.warn('⚠ WARNING: First transaction produced no logs! Contract may not be emitting events properly.');
            }

            successCount++;

            // Small delay to spread transactions across blocks
            if (i % 5 === 0) {
                await new Promise(resolve => setTimeout(resolve, 100));
            }
        } catch (error) {
            console.error(`[${i + 1}/50] Transaction failed:`, error.message);
        }
    }

    const currentBlock = await provider.getBlockNumber();
    console.log(`\n=== Test Data Generation Complete ===`);
    console.log(`Current block number: ${currentBlock}`);
    console.log(`Token contract: ${tokenAddress}`);
    console.log(`Successful transactions: ${successCount}/50`);
    console.log(`\nYou can now query the Flight server for logs and blocks!`);
    console.log(`\nExample queries:`);
    console.log(`- All logs: {"dataset": "logs", "startBlock": "0", "endBlock": "${currentBlock}"}`);
    console.log(`- Token logs only: {"dataset": "logs", "startBlock": "0", "endBlock": "${currentBlock}", "contractAddresses": ["${tokenAddress}"]}`);
    console.log(`- All blocks: {"dataset": "blocks", "startBlock": "0", "endBlock": "${currentBlock}"}`);

    // Query and display some logs to verify
    try {
        const logs = await provider.getLogs({
            fromBlock: 0,
            toBlock: currentBlock,
            address: tokenAddress
        });
        console.log(`\n=== Verification ===`);
        console.log(`Total logs generated: ${logs.length}`);
        if (logs.length > 0) {
            console.log(`First log block: ${logs[0].blockNumber}`);
            console.log(`Last log block: ${logs[logs.length - 1].blockNumber}`);
        }
    } catch (error) {
        console.error('Could not verify logs:', error.message);
    }
}

main()
    .then(() => {
        console.log('\nKeeping container alive for log inspection...');
        // Keep process alive so logs can be viewed
        setInterval(() => {}, 1000000);
    })
    .catch(error => {
        console.error('Error:', error);
        process.exit(1);
    });
