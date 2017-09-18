// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/namespace"
)

type tableNamespaceClassifier struct {
	nsInfo *namespacesInfo
}

func newTableNamespaceClassifier(nsInfo *namespacesInfo) tableNamespaceClassifier {
	return tableNamespaceClassifier{
		nsInfo,
	}
}

func (classifier tableNamespaceClassifier) GetAllNamespaces() []string {
	nsList := make([]string, len(classifier.nsInfo.namespaces))
	for name := range classifier.nsInfo.namespaces {
		nsList = append(nsList, name)
	}
	return nsList
}

func (classifier tableNamespaceClassifier) GetStoreNamespace(storeInfo *core.StoreInfo) string {
	for name, ns := range classifier.nsInfo.namespaces {
		if storeInfo.Id == ns.ID {
			return name
		}
	}
	return namespace.DefaultNamespace
}

func (classifier tableNamespaceClassifier) GetRegionNamespace(regionInfo *core.RegionInfo) string {
	for name, ns := range classifier.nsInfo.namespaces {
		startTable := core.DecodeTableID(regionInfo.StartKey)
		endTable := core.DecodeTableID(regionInfo.EndKey)
		for _, tableID := range ns.TableIDs {
			if tableID == startTable && tableID == endTable {
				return name
			}
		}
	}
	return namespace.DefaultNamespace
}
