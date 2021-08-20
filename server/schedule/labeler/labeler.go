// Copyright 2021 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package labeler

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"

	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/rangelist"
	"go.uber.org/zap"
)

// RegionLabeler is utility to label regions.
type RegionLabeler struct {
	storage *core.Storage
	sync.RWMutex
	labelRules map[string]*LabelRule
	rangeList  rangelist.List // sorted LabelRules of the type `KeyRange`
}

// NewRegionLabeler creates a Labeler instance.
func NewRegionLabeler(storage *core.Storage) (*RegionLabeler, error) {
	l := &RegionLabeler{
		storage:    storage,
		labelRules: make(map[string]*LabelRule),
	}

	if err := l.loadRules(); err != nil {
		return nil, err
	}
	return l, nil
}

func (l *RegionLabeler) loadRules() error {
	var toDelete []string
	err := l.storage.LoadRegionRules(func(k, v string) {
		var r LabelRule
		if err := json.Unmarshal([]byte(v), &r); err != nil {
			log.Error("failed to unmarshal label rule value", zap.String("rule-key", k), zap.String("rule-value", v), errs.ZapError(errs.ErrLoadRule))
			toDelete = append(toDelete, k)
			return
		}
		if err := l.adjustRule(&r); err != nil {
			log.Error("failed to adjust label rule", zap.String("rule-key", k), zap.String("rule-value", v), zap.Error(err))
			toDelete = append(toDelete, k)
			return
		}
		l.labelRules[r.ID] = &r
	})
	if err != nil {
		return err
	}
	for _, d := range toDelete {
		if err = l.storage.DeleteRegionRule(d); err != nil {
			return err
		}
	}
	l.buildRangeList()
	return nil
}

func (l *RegionLabeler) adjustRule(rule *LabelRule) error {
	if rule.ID == "" {
		return errs.ErrRegionRuleContent.FastGenByArgs("empty rule id")
	}
	if len(rule.Labels) == 0 {
		return errs.ErrRegionRuleContent.FastGenByArgs("no region labels")
	}
	for _, l := range rule.Labels {
		if l.Key == "" {
			return errs.ErrRegionRuleContent.FastGenByArgs("empty region label key")
		}
		if l.Value == "" {
			return errs.ErrRegionRuleContent.FastGenByArgs("empty region label value")
		}
	}

	switch rule.RuleType {
	case KeyRange:
		data, ok := rule.Rule.(map[string]interface{})
		if !ok {
			return errs.ErrRegionRuleContent.FastGenByArgs(fmt.Sprintf("invalid rule type: %T", reflect.TypeOf(rule.Rule)))
		}
		startKey, ok := data["start_key"].(string)
		if !ok {
			return errs.ErrRegionRuleContent.FastGenByArgs(fmt.Sprintf("invalid startKey type: %T", reflect.TypeOf(data["start_key"])))
		}
		endKey, ok := data["end_key"].(string)
		if !ok {
			return errs.ErrRegionRuleContent.FastGenByArgs(fmt.Sprintf("invalid endKey type: %T", reflect.TypeOf(data["end_key"])))
		}
		var r KeyRangeRule
		r.StartKeyHex, r.EndKeyHex = startKey, endKey
		var err error
		r.StartKey, err = hex.DecodeString(r.StartKeyHex)
		if err != nil {
			return errs.ErrHexDecodingString.FastGenByArgs(r.StartKeyHex)
		}
		r.EndKey, err = hex.DecodeString(r.EndKeyHex)
		if err != nil {
			return errs.ErrHexDecodingString.FastGenByArgs(r.EndKeyHex)
		}
		if len(r.EndKey) > 0 && bytes.Compare(r.EndKey, r.StartKey) <= 0 {
			return errs.ErrRegionRuleContent.FastGenByArgs("endKey should be greater than startKey")
		}
		rule.Rule = &r
		return nil
	}
	log.Error("invalid rule type", zap.String("rule-type", rule.RuleType))
	return errs.ErrRegionRuleContent.FastGenByArgs(fmt.Sprintf("invalid rule type: %s", rule.RuleType))
}

func (l *RegionLabeler) buildRangeList() {
	builder := rangelist.NewBuilder()
	for _, rule := range l.labelRules {
		if rule.RuleType == KeyRange {
			r := rule.Rule.(*KeyRangeRule)
			builder.AddItem(r.StartKey, r.EndKey, rule)
		}
	}
	l.rangeList = builder.Build()
}

// GetSplitKeys returns all split keys in the range (start, end).
func (l *RegionLabeler) GetSplitKeys(start, end []byte) [][]byte {
	l.RLock()
	defer l.RUnlock()
	return l.rangeList.GetSplitKeys(start, end)
}

// GetAllLabelRules returns all the rules.
func (l *RegionLabeler) GetAllLabelRules() []*LabelRule {
	l.RLock()
	defer l.RUnlock()
	rules := make([]*LabelRule, 0, len(l.labelRules))
	for _, rule := range l.labelRules {
		rules = append(rules, rule)
	}
	return rules
}

// GetLabelRules returns the rules that match the given ids.
func (l *RegionLabeler) GetLabelRules(ids []string) ([]*LabelRule, error) {
	l.RLock()
	defer l.RUnlock()
	rules := make([]*LabelRule, 0, len(ids))
	for _, id := range ids {
		if rule, ok := l.labelRules[id]; ok {
			rules = append(rules, rule)
		} else {
			return nil, errs.ErrRegionRuleNotFound.FastGenByArgs(id)
		}
	}
	return rules, nil
}

// GetLabelRule returns the Rule with the same ID.
func (l *RegionLabeler) GetLabelRule(id string) *LabelRule {
	l.RLock()
	defer l.RUnlock()
	return l.labelRules[id]
}

// SetLabelRule inserts or updates a LabelRule.
func (l *RegionLabeler) SetLabelRule(rule *LabelRule) error {
	l.Lock()
	defer l.Unlock()
	if err := l.adjustRule(rule); err != nil {
		return err
	}
	if err := l.storage.SaveRegionRule(rule.ID, rule); err != nil {
		return err
	}
	l.labelRules[rule.ID] = rule
	l.buildRangeList()
	return nil
}

// DeleteLabelRule removes a LabelRule.
func (l *RegionLabeler) DeleteLabelRule(id string) error {
	l.Lock()
	defer l.Unlock()
	if _, ok := l.labelRules[id]; !ok {
		return errs.ErrRegionRuleNotFound.FastGenByArgs(id)
	}
	if err := l.storage.DeleteRegionRule(id); err != nil {
		return err
	}
	delete(l.labelRules, id)
	l.buildRangeList()
	return nil
}

// Patch updates multiple region rules in a batch.
func (l *RegionLabeler) Patch(patch LabelRulePatch) error {
	for _, rule := range patch.SetRules {
		if err := l.adjustRule(rule); err != nil {
			return err
		}
	}

	// save to storage
	for _, key := range patch.DeleteRules {
		if err := l.storage.DeleteRegionRule(key); err != nil {
			return err
		}
	}
	for _, rule := range patch.SetRules {
		if err := l.storage.SaveRegionRule(rule.ID, rule); err != nil {
			return err
		}
	}

	// update inmemory states.
	l.Lock()
	defer l.Unlock()

	for _, key := range patch.DeleteRules {
		delete(l.labelRules, key)
	}
	for _, rule := range patch.SetRules {
		l.labelRules[rule.ID] = rule
	}
	l.buildRangeList()
	return nil
}

// GetRegionLabel returns the label of the region for a key.
func (l *RegionLabeler) GetRegionLabel(region *core.RegionInfo, key string) string {
	l.RLock()
	defer l.RUnlock()
	// search ranges
	if i, data := l.rangeList.GetData(region.GetStartKey(), region.GetEndKey()); i != -1 {
		for _, rule := range data {
			for _, l := range rule.(*LabelRule).Labels {
				if l.Key == key {
					return l.Value
				}
			}
		}
	}
	return ""
}

// GetRegionLabels returns the labels of the region.
func (l *RegionLabeler) GetRegionLabels(region *core.RegionInfo) []*RegionLabel {
	l.RLock()
	defer l.RUnlock()
	var result []*RegionLabel
	// search ranges
	if i, data := l.rangeList.GetData(region.GetStartKey(), region.GetEndKey()); i != -1 {
		for _, rule := range data {
			for _, l := range rule.(*LabelRule).Labels {
				result = append(result, &RegionLabel{
					Key:   l.Key,
					Value: l.Value,
				})
			}
		}
	}
	return result
}
