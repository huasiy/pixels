/*
 * Copyright 2022 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.executor.lambda.domain;

import io.pixelsdb.pixels.executor.join.JoinType;

/**
 * @author hank
 * @date 02/06/2022
 */
public class JoinInfo
{
    /**
     * The type of the join.
     */
    private JoinType joinType;
    /**
     * The alias of the columns from the small table in the join. These alias are used
     * as the column names in the join results. Join key columns are ignored if
     * {@link #outputJoinKeys} is false.
     */
    private String[] smallColumnAlias;
    /**
     * The alias of the columns from the large table in the join. These alias are used
     * as the column names in the join results. Join key columns are ignored if
     * {@link #joinType} is NATURAL or {@link #outputJoinKeys} is false.
     */
    private String[] largeColumnAlias;
    /**
     * Whether the join result contains the join keys from the left and right tables.
     */
    private boolean outputJoinKeys;
    /**
     * Whether the join output has to be partitioned.
     */
    private boolean postPartition = false;
    /**
     * The partition information of the output if outputPartitioned is true.
     */
    private PartitionInfo postPartitionInfo;

    /**
     * Default constructor for Jackson.
     */
    public JoinInfo() { }

    public JoinInfo(JoinType joinType, String[] smallColumnAlias, String[] largeColumnAlias,
                    boolean outputJoinKeys, boolean postPartition, PartitionInfo postPartitionInfo)
    {
        this.joinType = joinType;
        this.smallColumnAlias = smallColumnAlias;
        this.largeColumnAlias = largeColumnAlias;
        this.outputJoinKeys = outputJoinKeys;
        this.postPartition = postPartition;
        this.postPartitionInfo = postPartitionInfo;
    }

    public JoinType getJoinType()
    {
        return joinType;
    }

    public void setJoinType(JoinType joinType)
    {
        this.joinType = joinType;
    }

    public String[] getSmallColumnAlias()
    {
        return smallColumnAlias;
    }

    public void setSmallColumnAlias(String[] smallColumnAlias)
    {
        this.smallColumnAlias = smallColumnAlias;
    }

    public String[] getLargeColumnAlias()
    {
        return largeColumnAlias;
    }

    public void setLargeColumnAlias(String[] largeColumnAlias)
    {
        this.largeColumnAlias = largeColumnAlias;
    }

    public boolean isOutputJoinKeys()
    {
        return outputJoinKeys;
    }

    public void setOutputJoinKeys(boolean outputJoinKeys)
    {
        this.outputJoinKeys = outputJoinKeys;
    }

    public boolean isPostPartition()
    {
        return postPartition;
    }

    public void setPostPartition(boolean postPartition)
    {
        this.postPartition = postPartition;
    }

    public PartitionInfo getPostPartitionInfo()
    {
        return postPartitionInfo;
    }

    public void setPostPartitionInfo(PartitionInfo postPartitionInfo)
    {
        this.postPartitionInfo = postPartitionInfo;
    }
}