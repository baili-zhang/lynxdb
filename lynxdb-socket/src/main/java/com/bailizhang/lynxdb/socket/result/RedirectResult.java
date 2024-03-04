/*
 * Copyright 2022-2023 Baili Zhang.
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

package com.bailizhang.lynxdb.socket.result;

import com.bailizhang.lynxdb.core.common.DataBlocks;
import com.bailizhang.lynxdb.socket.client.ServerNode;

import java.nio.ByteBuffer;

import static com.bailizhang.lynxdb.ldtp.result.ResultType.REDIRECT;

public record RedirectResult(ServerNode other) {
    public ByteBuffer[] toBuffers() {
        DataBlocks dataBlocks = new DataBlocks(true);

        dataBlocks.appendRawByte(REDIRECT);
        dataBlocks.appendVarStr(other.toString());

        return dataBlocks.toBuffers();
    }
}
