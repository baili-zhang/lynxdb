/*
 * Copyright 2023 Baili Zhang.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bailizhang.lynxdb.ldtp.result;

public interface RaftRpcResult {
    byte PRE_VOTE_RESULT            = (byte) 0x01;
    byte REQUEST_VOTE_RESULT        = (byte) 0x02;
    byte APPEND_ENTRIES_RESULT      = (byte) 0x03;
    byte INSTALL_SNAPSHOT_RESULT    = (byte) 0x04;
    byte LEADER_NOT_EXISTED_RESULT  = (byte) 0x05;
    byte JOIN_CLUSTER_RESULT        = (byte) 0x06;
}
