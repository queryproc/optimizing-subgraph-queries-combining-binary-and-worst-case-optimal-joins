package ca.waterloo.dsg.graphflow.plan.operator.hashjoin;

import lombok.var;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class HashTable implements Serializable {

    static class BlockInfo implements Serializable {
        int[] block;
        int startOffset;
        int endOffset;
    }

    transient private int[][] blocks;
    transient private List<int[]> extraBlocks;
    transient private int[][] blockIdsAndChunkOffsets;
    transient int[] numChunks;
    private int INITIAL_NUM_BLOCKS = 10;//1000;
    private int NUM_TUPLES_PER_CHUNK = 32;//64;
    private int NUM_CHUNKS_PER_BLOCK = 80;//8000;
    private int INITIAL_NUM_CHUNKS_PER_VERTEX = 6;

    private int BLOCK_SZ;
    private int CHUNK_SZ;

    private int nextGlobalBlockId = 0;
    private int nextGlobalChunkOffset = 0;

    private int buildHashIdx;
    private int buildTupleLen;
    private int hashedTupleLen;

    /**
     * Constructs a {@link HashTable} object.
     *
     * @param buildHashIdx is the index of the vertex value in the tuples being hashed.
     * @param hashedTupleLen is the size of the tuple being hashed minus 1.
     */
    public HashTable(int buildHashIdx, int hashedTupleLen) {
        this.buildHashIdx = buildHashIdx;
        this.buildTupleLen = hashedTupleLen + 1;
        this.hashedTupleLen = hashedTupleLen;
        CHUNK_SZ = NUM_TUPLES_PER_CHUNK * hashedTupleLen;
        BLOCK_SZ = CHUNK_SZ * NUM_CHUNKS_PER_BLOCK;
    }

    void setInitialNumBlocks(int initialNumBlocks) {
        INITIAL_NUM_BLOCKS = initialNumBlocks;
    }

    void setNumTuplesPerChunk(int numTuplesPerChunk) {
        NUM_TUPLES_PER_CHUNK = numTuplesPerChunk;
    }

    void setNumChunksPerBlock(int numChunksPerBlock) {
        NUM_CHUNKS_PER_BLOCK = numChunksPerBlock;
    }

    /**
     * Allocates the initial memory required by the {@link HashTable}.
     *
     * @param highestVertexId is the highest vertex id in the input data graph.
     */
    void allocateInitialMemory(int highestVertexId) {
        blocks = new int[INITIAL_NUM_BLOCKS][BLOCK_SZ];
        extraBlocks = new ArrayList<>(INITIAL_NUM_BLOCKS);
        blockIdsAndChunkOffsets = new int[highestVertexId + 1][INITIAL_NUM_CHUNKS_PER_VERTEX * 3];
        numChunks = new int[highestVertexId + 1];
    }

    /**
     * insert a tuple in the {@link HashTable}.
     *
     * @param buildTuple is the tuple to hash.
     */
    void insertTuple(int[] buildTuple) {
        int hashVertex = buildTuple[buildHashIdx];
        var lastChunkIdx = this.numChunks[hashVertex];
        if (0 == lastChunkIdx) {
            this.numChunks[hashVertex]++;
            updateBlockIdsAndGlobalAndChunkOffset(hashVertex);
        }
        lastChunkIdx = 3 * (this.numChunks[hashVertex] - 1);
        var blockId = blockIdsAndChunkOffsets[hashVertex][lastChunkIdx];
        var startOffset = blockIdsAndChunkOffsets[hashVertex][lastChunkIdx + 1];
        var endOffset = blockIdsAndChunkOffsets[hashVertex][lastChunkIdx + 2];
        var block = blockId < INITIAL_NUM_BLOCKS ? blocks[blockId] :
            extraBlocks.get(blockId - INITIAL_NUM_BLOCKS);
        for (var i = 0; i < buildTupleLen; i++) {
            if (i != buildHashIdx) {
                block[endOffset++] = buildTuple[i];
            }
        }
        blockIdsAndChunkOffsets[hashVertex][lastChunkIdx + 2] = endOffset;
        if (CHUNK_SZ <= (endOffset - startOffset  + hashedTupleLen)) {
            this.numChunks[hashVertex]++;
            resizeBlockIdsAndGlobalAndChunkOffset(hashVertex);
            updateBlockIdsAndGlobalAndChunkOffset(hashVertex);
        }
    }

    /**
     * Sets the block, the start and the end offsets in the passed {@link BlockInfo}.
     *
     * @param hashVertex is the value of the hashed vertex.
     * @param chunkIdx is the index of the chunk.
     * @param blockInfo is the object to setAdjListSortOrder.
     */
    void getBlockAndOffsets(int hashVertex, int chunkIdx, BlockInfo blockInfo) {
        var blockId = blockIdsAndChunkOffsets[hashVertex][chunkIdx * 3];
        blockInfo.startOffset = blockIdsAndChunkOffsets[hashVertex][chunkIdx * 3 + 1];
        blockInfo.endOffset = blockIdsAndChunkOffsets[hashVertex][chunkIdx * 3 + 2];
        blockInfo.block = blockId < INITIAL_NUM_BLOCKS ? blocks[blockId] :
            extraBlocks.get(blockId - INITIAL_NUM_BLOCKS);
    }

    private void resizeBlockIdsAndGlobalAndChunkOffset(int hashVertex) {
        if (this.numChunks[hashVertex] + 1 > (blockIdsAndChunkOffsets[hashVertex].length / 3)) {
            var newChunkBlockIdOffsetArray = new int[(this.numChunks[hashVertex] + 2) * 3];
            System.arraycopy(blockIdsAndChunkOffsets[hashVertex], 0, newChunkBlockIdOffsetArray, 0,
                blockIdsAndChunkOffsets[hashVertex].length);
            blockIdsAndChunkOffsets[hashVertex] = newChunkBlockIdOffsetArray;
        }
    }

    private void updateBlockIdsAndGlobalAndChunkOffset(int hashVertex) {
        var lastChunkIdx = (this.numChunks[hashVertex] - 1) * 3;
        blockIdsAndChunkOffsets[hashVertex][lastChunkIdx] = nextGlobalBlockId;
        blockIdsAndChunkOffsets[hashVertex][lastChunkIdx + 1] = nextGlobalChunkOffset;
        blockIdsAndChunkOffsets[hashVertex][lastChunkIdx + 2] = nextGlobalChunkOffset;
        nextGlobalChunkOffset += CHUNK_SZ;
        if (nextGlobalChunkOffset == BLOCK_SZ) {
            nextGlobalBlockId++;
            if (nextGlobalBlockId >= INITIAL_NUM_BLOCKS) {
                extraBlocks.add(new int[BLOCK_SZ]);
            }
            nextGlobalChunkOffset = 0;
        }
    }
}
