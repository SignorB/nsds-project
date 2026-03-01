package DistrictNodeManager;

import org.jetbrains.annotations.NotNull;

/**
 * Represents a device in the smart grid
 * Node type is one of: "producer", "consumer", "accumulator"
 *
 * @param nodeId     unique identifier, format "n_<number>"
 * @param userId     owner of this node
 * @param districtId district this node belongs to
 * @param nodeType   role of this node in the grid
 */
public record Node(String nodeId, String userId, String districtId, String nodeType) {

    @Override
    public @NotNull String toString() {
        return "Node{nodeId='" + nodeId + "', userId='" + userId + "', districtId='" + districtId + "', nodeType='" + nodeType + "'}";
    }
}
