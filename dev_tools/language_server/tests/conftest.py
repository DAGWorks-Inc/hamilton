############################################################################
# Original work Copyright 2017 Palantir Technologies, Inc.                 #
# Original work licensed under the MIT License.                            #
# See ThirdPartyNotices.txt in the project root for license information.   #
# All modifications Copyright (c) Open Law Library. All rights reserved.   #
#                                                                          #
# Licensed under the Apache License, Version 2.0 (the "License")           #
# you may not use this file except in compliance with the License.         #
# You may obtain a copy of the License at                                  #
#                                                                          #
#     http: // www.apache.org/licenses/LICENSE-2.0                         #
#                                                                          #
# Unless required by applicable law or agreed to in writing, software      #
# distributed under the License is distributed on an "AS IS" BASIS,        #
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. #
# See the License for the specific language governing permissions and      #
# limitations under the License.                                           #
############################################################################
import pytest
from hamilton_lsp.server import HamiltonLanguageServer, regiser_server_features

from .ls_setup import NativeClientServer


@pytest.fixture(autouse=False)
def client_server():
    client_server = NativeClientServer(LS=HamiltonLanguageServer)
    regiser_server_features(client_server.server)

    client_server.start()
    client, server = client_server

    yield client, server

    client_server.stop()
