/*
Copyright 2025 Priyo Lahiri.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

// Two CRs of one kind that give the same name on one target (operations.md id
// 217). Neo4j reads database, composite and alias names ignoring case (it
// lower-cases them; a sharded family's are lower-cased too, rules 48, 53,
// 146), so "Sales" and "sales" are one database. Two Neo4jDatabase CRs for it
// both reported Ready on it, and deleting either dropped the database the
// other still named; two Neo4jShardedDatabase CRs adopted the same family.
// As for Neo4jPlugin (id 195): the oldest CR owns the name, the others are
// Failed with one DuplicateName Warning naming it and act on nothing, a
// deletion leaves the database to a CR that still names it, and a watch on
// the kind wakes the others of a name when one of them comes, goes or is
// renamed.

import (
	"context"
	"fmt"
	"strings"

	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// nameClaim is one CR's claim on a name in Neo4j on its target.
type nameClaim struct {
	object   client.Object
	target   string // spec.clusterRef
	name     string // the name in Neo4j, as the CR gives it
	deleting bool
}

// sameName reports whether b claims a's name on a's target, as Neo4j reads
// it, and is another CR.
func (a nameClaim) sameName(b nameClaim) bool {
	return b.object.GetName() != a.object.GetName() && b.target == a.target && strings.EqualFold(b.name, a.name)
}

// ownsOver reports whether a owns a contested name over b: the older
// creationTimestamp, then the lower UID — the same order on every replica.
func (a nameClaim) ownsOver(b nameClaim) bool {
	ta, tb := a.object.GetCreationTimestamp(), b.object.GetCreationTimestamp()
	if !ta.Equal(&tb) {
		return ta.Before(&tb)
	}
	return string(a.object.GetUID()) < string(b.object.GetUID())
}

// nameOwner returns the metadata.name of the CR that owns self's name, of the
// claims of self's kind in its namespace: self when no live claim contests
// it. A CR being deleted contests nothing.
func nameOwner(self nameClaim, claims []nameClaim) string {
	owner := self
	for _, c := range claims {
		if c.deleting || !self.sameName(c) {
			continue
		}
		if c.ownsOver(owner) {
			owner = c
		}
	}
	return owner.object.GetName()
}

// otherLiveClaim returns the metadata.name of a live CR, other than self,
// that still gives self's name: a deletion leaves the database to it. "" when
// none does.
func otherLiveClaim(self nameClaim, claims []nameClaim) string {
	var owner *nameClaim
	for i := range claims {
		c := claims[i]
		if c.deleting || !self.sameName(c) {
			continue
		}
		if owner == nil || c.ownsOver(*owner) {
			owner = &c
		}
	}
	if owner == nil {
		return ""
	}
	return owner.object.GetName()
}

// nameClaimKind is one kind whose CRs claim a name in Neo4j.
type nameClaimKind struct {
	kind string // the CR kind, as a message names it
	what string // what the name is in Neo4j, as a message names it
	list func(ctx context.Context, c client.Reader, namespace string) ([]client.Object, error)
	// of returns obj's target and its name in Neo4j; false for another kind.
	of func(obj client.Object) (target, name string, ok bool)
}

func (k nameClaimKind) claim(obj client.Object) (nameClaim, bool) {
	target, name, ok := k.of(obj)
	if !ok {
		return nameClaim{}, false
	}
	return nameClaim{object: obj, target: target, name: name, deleting: obj.GetDeletionTimestamp() != nil}, true
}

func (k nameClaimKind) claims(ctx context.Context, c client.Reader, namespace string) ([]nameClaim, error) {
	objs, err := k.list(ctx, c, namespace)
	if err != nil {
		return nil, fmt.Errorf("failed to list %ss in namespace %q: %w", k.kind, namespace, err)
	}
	out := make([]nameClaim, 0, len(objs))
	for _, o := range objs {
		if cl, ok := k.claim(o); ok {
			out = append(out, cl)
		}
	}
	return out, nil
}

// duplicate returns the status message of obj when an older CR of its kind
// owns its name on its target, "" when obj owns it.
func (k nameClaimKind) duplicate(ctx context.Context, c client.Reader, obj client.Object) (string, error) {
	self, ok := k.claim(obj)
	if !ok {
		return "", nil
	}
	claims, err := k.claims(ctx, c, obj.GetNamespace())
	if err != nil {
		return "", err
	}
	owner := nameOwner(self, claims)
	if owner == obj.GetName() {
		return "", nil
	}
	return fmt.Sprintf("duplicate %s: %q (older) already gives %s %q on %s, and Neo4j's names ignore case; "+
		"delete or rename one of them before this one can reconcile",
		k.kind, owner, k.what, self.name, targetRefDisplay(self.target)), nil
}

// keptFor returns the event of obj's deletion when another live CR of its
// kind still gives its name on its target — the deletion leaves the name in
// Neo4j to that CR — and "" when none does.
func (k nameClaimKind) keptFor(ctx context.Context, c client.Reader, obj client.Object) (string, error) {
	self, ok := k.claim(obj)
	if !ok {
		return "", nil
	}
	claims, err := k.claims(ctx, c, obj.GetNamespace())
	if err != nil {
		return "", err
	}
	other := otherLiveClaim(self, claims)
	if other == "" {
		return "", nil
	}
	return fmt.Sprintf("%s %q kept, not dropped: %s %q still gives it", k.what, self.name, k.kind, other), nil
}

// othersOfTheName maps a CR to the other CRs of its kind in its namespace that
// give its name on its target. On an update the handler maps the old object
// too, so a CR renamed away from a name wakes the ones it leaves.
func (k nameClaimKind) othersOfTheName(c client.Reader) handler.EventHandler {
	return handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
		self, ok := k.claim(obj)
		if !ok {
			return nil
		}
		claims, err := k.claims(ctx, c, obj.GetNamespace())
		if err != nil {
			log.FromContext(ctx).Error(err, "Failed to list the CRs that give a name", "kind", k.kind,
				"name", obj.GetName(), "namespace", obj.GetNamespace())
			return nil
		}
		var requests []reconcile.Request
		for _, cl := range claims {
			if self.sameName(cl) {
				requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(cl.object)})
			}
		}
		return requests
	})
}

// nameClaimChanged passes the events that can change which CR owns a name: a
// CR created or gone, one whose spec changed, and one whose deletion started.
// Status writes, which every pass makes, do not pass.
var nameClaimChanged = predicate.Funcs{
	CreateFunc: func(event.CreateEvent) bool { return true },
	DeleteFunc: func(event.DeleteEvent) bool { return true },
	UpdateFunc: func(e event.UpdateEvent) bool {
		if e.ObjectOld == nil || e.ObjectNew == nil {
			return false
		}
		return e.ObjectOld.GetGeneration() != e.ObjectNew.GetGeneration() ||
			(e.ObjectOld.GetDeletionTimestamp() == nil) != (e.ObjectNew.GetDeletionTimestamp() == nil)
	},
	GenericFunc: func(event.GenericEvent) bool { return false },
}

// onClaimChange is the watch option of othersOfTheName: nameClaimChanged.
func (k nameClaimKind) onClaimChange() builder.WatchesOption {
	return builder.WithPredicates(nameClaimChanged)
}

var databaseNames = nameClaimKind{
	kind: "Neo4jDatabase", what: "database",
	list: func(ctx context.Context, c client.Reader, ns string) ([]client.Object, error) {
		l := &neo4jv1beta1.Neo4jDatabaseList{}
		if err := c.List(ctx, l, client.InNamespace(ns)); err != nil {
			return nil, err
		}
		out := make([]client.Object, 0, len(l.Items))
		for i := range l.Items {
			out = append(out, &l.Items[i])
		}
		return out, nil
	},
	of: func(obj client.Object) (string, string, bool) {
		db, ok := obj.(*neo4jv1beta1.Neo4jDatabase)
		if !ok {
			return "", "", false
		}
		return db.Spec.ClusterRef, db.Spec.Name, true
	},
}

var shardedDatabaseNames = nameClaimKind{
	kind: "Neo4jShardedDatabase", what: "sharded database",
	list: func(ctx context.Context, c client.Reader, ns string) ([]client.Object, error) {
		l := &neo4jv1beta1.Neo4jShardedDatabaseList{}
		if err := c.List(ctx, l, client.InNamespace(ns)); err != nil {
			return nil, err
		}
		out := make([]client.Object, 0, len(l.Items))
		for i := range l.Items {
			out = append(out, &l.Items[i])
		}
		return out, nil
	},
	of: func(obj client.Object) (string, string, bool) {
		sd, ok := obj.(*neo4jv1beta1.Neo4jShardedDatabase)
		if !ok {
			return "", "", false
		}
		return sd.Spec.ClusterRef, sd.Spec.Name, true
	},
}

var compositeDatabaseNames = nameClaimKind{
	kind: "Neo4jCompositeDatabase", what: "composite database",
	list: func(ctx context.Context, c client.Reader, ns string) ([]client.Object, error) {
		l := &neo4jv1beta1.Neo4jCompositeDatabaseList{}
		if err := c.List(ctx, l, client.InNamespace(ns)); err != nil {
			return nil, err
		}
		out := make([]client.Object, 0, len(l.Items))
		for i := range l.Items {
			out = append(out, &l.Items[i])
		}
		return out, nil
	},
	of: func(obj client.Object) (string, string, bool) {
		cd, ok := obj.(*neo4jv1beta1.Neo4jCompositeDatabase)
		if !ok {
			return "", "", false
		}
		return cd.Spec.ClusterRef, compositeName(cd), true
	},
}

var aliasNames = nameClaimKind{
	kind: "Neo4jDatabaseAlias", what: "alias",
	list: func(ctx context.Context, c client.Reader, ns string) ([]client.Object, error) {
		l := &neo4jv1beta1.Neo4jDatabaseAliasList{}
		if err := c.List(ctx, l, client.InNamespace(ns)); err != nil {
			return nil, err
		}
		out := make([]client.Object, 0, len(l.Items))
		for i := range l.Items {
			out = append(out, &l.Items[i])
		}
		return out, nil
	},
	of: func(obj client.Object) (string, string, bool) {
		a, ok := obj.(*neo4jv1beta1.Neo4jDatabaseAlias)
		if !ok {
			return "", "", false
		}
		return a.Spec.ClusterRef, effectiveAliasName(a), true
	},
}
