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

package com.bailizhang.lynxdb.ldtp.request;

public interface RaftRpc {
    byte PRE_VOTE               = (byte) 0x01;
    byte REQUEST_VOTE           = (byte) 0x02;
    byte APPEND_ENTRIES         = (byte) 0x03;
    byte INSTALL_SNAPSHOT       = (byte) 0x04;
    byte JOIN_CLUSTER           = (byte) 0x05;
    byte LEAVE_CLUSTER          = (byte) 0x06;
}
