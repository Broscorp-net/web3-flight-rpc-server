package net.broscorp.web3.dto.request;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Data;

import java.math.BigInteger;

/**
 * Base class for all client requests sent via FlightDescriptor/Ticket.
 * <p>
 *     Treats {@code dataset} property as the discriminator value between different datasets.
 * </p>
 */
@Data
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "dataset"
)
@JsonSubTypes({
        @JsonSubTypes.Type(value = BlocksRequest.class, name = "blocks"),
        @JsonSubTypes.Type(value = LogsRequest.class, name = "logs")
})
sealed public abstract class ClientRequest permits BlocksRequest, LogsRequest {
    private String dataset;
    private BigInteger startBlock;
    private BigInteger endBlock;

    public boolean needsHistoricalData() {
        return startBlock != null;
    }

    public boolean awaitingForRealTimeData() {
        return endBlock == null;
    }
}
