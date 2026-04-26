package net.broscorp.web3.service;

import java.math.BigInteger;
import org.web3j.protocol.core.methods.response.TransactionReceipt;
import org.web3j.utils.Numeric;

/**
 * web3j 5.0.0's {@link TransactionReceipt} dropped the {@code effectiveGasPrice}
 * field that EIP-1559 nodes return on {@code eth_getBlockReceipts}. This subclass
 * captures it so we can compute per-tx fee = gasUsed * effectiveGasPrice.
 */
public class ExtendedTransactionReceipt extends TransactionReceipt {

    private String effectiveGasPrice;

    public String getEffectiveGasPriceRaw() {
        return effectiveGasPrice;
    }

    public BigInteger getEffectiveGasPrice() {
        return effectiveGasPrice == null
            ? null
            : Numeric.decodeQuantity(effectiveGasPrice);
    }

    public void setEffectiveGasPrice(String effectiveGasPrice) {
        this.effectiveGasPrice = effectiveGasPrice;
    }
}
