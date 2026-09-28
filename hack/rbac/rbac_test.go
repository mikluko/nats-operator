package main

import (
	"fmt"
	"go/ast"
	"go/constant"
	"go/parser"
	"go/token"
	"go/types"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/callgraph"
	"golang.org/x/tools/go/callgraph/rta"
	"golang.org/x/tools/go/packages"
	"golang.org/x/tools/go/ssa"
	"golang.org/x/tools/go/ssa/ssautil"
	rbacv1 "k8s.io/api/rbac/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/yaml"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
	jetstreamv1beta1 "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/manager"
)

const root = "../.."

const (
	clientPkg         = "sigs.k8s.io/controller-runtime/pkg/client"
	controllerutilPkg = "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	builderPkg        = "sigs.k8s.io/controller-runtime/pkg/builder"
	unstructuredPkg   = "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// unstructuredKinds maps each package-level GroupVersionKind the scanned
// code sets on an unstructured object to the resource it names.
var unstructuredKinds = map[string]string{
	module + "/internal/natscluster.certificateGVK": "cert-manager.io/certificates",
}

// libraryGrants maps each library function whose API calls the scan cannot
// see to the resources it creates.
var libraryGrants = map[string][]string{
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters.WithAuthenticationAndAuthorization": {
		"authentication.k8s.io/tokenreviews",
		"authorization.k8s.io/subjectaccessreviews",
	},
}

// clientVerbs is the verb each method of controller-runtime's client
// interfaces requires on its object.
var clientVerbs = map[string]string{
	"Get":         "get",
	"List":        "list",
	"Create":      "create",
	"Update":      "update",
	"Patch":       "patch",
	"Apply":       "patch",
	"Delete":      "delete",
	"DeleteAllOf": "deletecollection",
}

// TestRBAC_MarkersMatchCode pins each controller's generated ClusterRole to
// exactly what the code its command reaches requires. A call whose object
// the scan cannot trace to a type fails the test.
func TestRBAC_MarkersMatchCode(t *testing.T) {
	owned, err := ownedPackages(root)
	require.NoError(t, err)
	s := newScan(t)
	for c := range owned {
		t.Run(c, func(t *testing.T) {
			required, metadata := s.required(t, c)
			require.Equal(t, metadataWatches[c], metadata, "metadata-only watches")
			for _, verb := range []string{"list", "watch"} {
				if pos, ok := required["/secrets"][verb]; ok {
					require.Contains(t, metadata, sitePackage(t, pos), "%s on Secrets is first required at %s, which is no metadata-only watch", verb, pos)
				}
			}
			granted := generatedRole(t, c)
			for key, verbs := range required {
				for verb, pos := range verbs {
					if !slices.Contains(granted[key], verb) {
						t.Errorf("%s requires %s on %s, which %s is not granted", pos, verb, key, c)
					}
				}
			}
			for key, verbs := range granted {
				for _, verb := range verbs {
					if _, ok := required[key][verb]; !ok {
						t.Errorf("%s is granted %s on %s, which no code it reaches requires", c, verb, key)
					}
				}
			}
		})
	}
}

// TestRBAC_MarkersInOwnedPackages pins that every +kubebuilder:rbac marker
// sits in a package whose markers make some controller's ClusterRole.
func TestRBAC_MarkersInOwnedPackages(t *testing.T) {
	owned, err := ownedPackages(root)
	require.NoError(t, err)
	var all []string
	for _, ps := range owned {
		all = append(all, ps...)
	}
	for _, dir := range []string{"api", "cmd", "internal"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(p string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
				return err
			}
			f, err := parser.ParseFile(token.NewFileSet(), p, nil, parser.ParseComments|parser.SkipObjectResolution)
			if err != nil {
				return err
			}
			if !hasMarker(f) {
				return nil
			}
			rel, err := filepath.Rel(root, filepath.Dir(p))
			if err != nil {
				return err
			}
			require.Contains(t, all, module+"/"+filepath.ToSlash(rel), "%s carries an RBAC marker no controller reads", p)
			return nil
		})
		require.NoError(t, err)
	}
}

func hasMarker(f *ast.File) bool {
	for _, g := range f.Comments {
		for _, c := range g.List {
			if strings.HasPrefix(strings.TrimSpace(strings.TrimPrefix(c.Text, "//")), "+kubebuilder:rbac") {
				return true
			}
		}
	}
	return false
}

// grants maps "group/resource" to the verbs on it.
type grants map[string][]string

// generatedRole returns the rules of config/rbac/<c>/role.yaml.
func generatedRole(t *testing.T, c string) grants {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(root, "config", "rbac", c, "role.yaml"))
	require.NoError(t, err)
	var role rbacv1.ClusterRole
	require.NoError(t, yaml.Unmarshal(b, &role))
	g := grants{}
	for _, r := range role.Rules {
		require.Empty(t, r.ResourceNames)
		require.Empty(t, r.NonResourceURLs)
		for _, group := range r.APIGroups {
			for _, res := range r.Resources {
				key := group + "/" + res
				require.NotContains(t, g, key)
				g[key] = slices.Sorted(slices.Values(r.Verbs))
			}
		}
	}
	return g
}

// metadataWatches is, per controller, the package of each WatchesMetadata
// call its command reaches, sorted, one entry per call.
var metadataWatches = map[string][]string{
	"auth-controller":      {"internal/authctl", "internal/authctl", "internal/authctl", "internal/authctl"},
	"cluster-controller":   {"internal/natscluster"},
	"jetstream-controller": {"internal/natsconn"},
}

// sitePackage returns the module-relative directory of the file position pos
// names.
func sitePackage(t *testing.T, pos string) string {
	file, _, _ := strings.Cut(pos, ":")
	rel, err := filepath.Rel(moduleDir(t), filepath.Dir(file))
	require.NoError(t, err)
	return filepath.ToSlash(rel)
}

// moduleDir returns root as the absolute, symlink-free path positions carry.
func moduleDir(t *testing.T) string {
	abs, err := filepath.Abs(root)
	require.NoError(t, err)
	abs, err = filepath.EvalSymlinks(abs)
	require.NoError(t, err)
	return abs
}

// scan is the SSA form of every controller command and what it links.
type scan struct {
	prog     *ssa.Program
	uncached map[string]bool
	mains    map[string]*ssa.Package
	kinds    map[string]schema.GroupVersionKind
	plurals  map[schema.GroupKind]string
	fields   map[string][]ssa.Value
	located  map[fieldLoc][]ssa.Value
	allocs   map[ssa.Value][]ssa.Value
	globals  map[*ssa.Global][]ssa.Value
	closures map[*ssa.Function][]*ssa.MakeClosure
}

func newScan(t *testing.T) *scan {
	t.Helper()
	pkgs, err := packages.Load(&packages.Config{Mode: packages.LoadAllSyntax, Dir: root}, "./cmd/...")
	require.NoError(t, err)
	for _, p := range pkgs {
		require.Empty(t, p.Errors, p.PkgPath)
	}
	prog, ssaPkgs := ssautil.AllPackages(pkgs, ssa.InstantiateGenerics)
	prog.Build()
	s := &scan{
		prog:     prog,
		mains:    map[string]*ssa.Package{},
		uncached: uncachedKinds(),
		kinds:    schemeKinds(t),
		plurals:  crdPlurals(t),
		fields:   map[string][]ssa.Value{},
		located:  map[fieldLoc][]ssa.Value{},
		allocs:   map[ssa.Value][]ssa.Value{},
		globals:  map[*ssa.Global][]ssa.Value{},
		closures: map[*ssa.Function][]*ssa.MakeClosure{},
	}
	for _, p := range ssaPkgs {
		if p != nil && p.Pkg.Name() == "main" && strings.HasSuffix(p.Pkg.Path(), "-controller") {
			s.mains[path.Base(p.Pkg.Path())] = p
		}
	}
	for fn := range ssautil.AllFunctions(prog) {
		s.index(fn)
	}
	return s
}

// index records what every store and closure in fn puts where.
func (s *scan) index(fn *ssa.Function) {
	for _, b := range fn.Blocks {
		for _, instr := range b.Instrs {
			switch i := instr.(type) {
			case *ssa.Store:
				switch a := i.Addr.(type) {
				case *ssa.FieldAddr:
					k := fieldKey(a.X.Type(), a.Field)
					s.fields[k] = append(s.fields[k], i.Val)
					if root := storeRoot(a.X); root != nil {
						l := fieldLoc{root, a.Field}
						s.located[l] = append(s.located[l], i.Val)
					}
				case *ssa.IndexAddr:
					base := arrayBase(a.X)
					s.allocs[base] = append(s.allocs[base], i.Val)
				case *ssa.Global:
					s.globals[a] = append(s.globals[a], i.Val)
				case *ssa.Alloc:
					s.allocs[a] = append(s.allocs[a], i.Val)
				}
			case *ssa.MakeClosure:
				f := i.Fn.(*ssa.Function)
				s.closures[f] = append(s.closures[f], i)
			}
		}
	}
}

// fieldLoc is field i of the struct values rooted at one Alloc or Global.
type fieldLoc struct {
	root  ssa.Value
	field int
}

// storeRoot returns the Alloc or Global that the struct pointer p points
// into, or nil where that is not plain from p.
func storeRoot(p ssa.Value) ssa.Value {
	if ia, ok := p.(*ssa.IndexAddr); ok {
		p = arrayBase(ia.X)
	}
	switch p.(type) {
	case *ssa.Alloc, *ssa.Global:
		return p
	}
	return nil
}

// fieldKey names field i of the struct t points to or is.
func fieldKey(t types.Type, i int) string {
	if p, ok := t.Underlying().(*types.Pointer); ok {
		t = p.Elem()
	}
	return fmt.Sprintf("%s#%d", types.TypeString(t, nil), i)
}

// arrayBase follows slicing back to the value a slice's elements live in.
func arrayBase(v ssa.Value) ssa.Value {
	for {
		sl, ok := v.(*ssa.Slice)
		if !ok {
			return v
		}
		v = sl.X
	}
}

// uncachedKinds holds "pkgpath.Name" of every type manager.ClientOptions
// makes the client read from the API server.
func uncachedKinds() map[string]bool {
	out := map[string]bool{}
	for _, obj := range manager.ClientOptions().Cache.DisableFor {
		out[typeName(reflect.TypeOf(obj).Elem())] = true
	}
	return out
}

// schemeKinds maps "pkgpath.Name" of every Go type the controllers' schemes
// know to its GroupVersionKind, a list type to its item's.
func schemeKinds(t *testing.T) map[string]schema.GroupVersionKind {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{
		clientgoscheme.AddToScheme,
		authv1beta1.AddToScheme,
		clusterv1beta1.AddToScheme,
		jetstreamv1beta1.AddToScheme,
		natsv1beta1.AddToScheme,
	} {
		require.NoError(t, add(scheme))
	}
	out := map[string]schema.GroupVersionKind{}
	for gvk, rt := range scheme.AllKnownTypes() {
		if gvk.Version == runtime.APIVersionInternal {
			continue
		}
		if item, ok := strings.CutSuffix(gvk.Kind, "List"); ok && scheme.Recognizes(gvk.GroupVersion().WithKind(item)) {
			gvk.Kind = item
		}
		out[typeName(rt)] = gvk
	}
	return out
}

func typeName(rt reflect.Type) string {
	return rt.PkgPath() + "." + rt.Name()
}

// crdPlurals maps each CRD's group and kind to its plural.
func crdPlurals(t *testing.T) map[schema.GroupKind]string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(root, "config", "crd", "*.yaml"))
	require.NoError(t, err)
	require.NotEmpty(t, files)
	out := map[schema.GroupKind]string{}
	for _, f := range files {
		b, err := os.ReadFile(f)
		require.NoError(t, err)
		var crd apiextensionsv1.CustomResourceDefinition
		require.NoError(t, yaml.Unmarshal(b, &crd))
		out[schema.GroupKind{Group: crd.Spec.Group, Kind: crd.Spec.Names.Kind}] = crd.Spec.Names.Plural
	}
	return out
}

// requirement collects "group/resource" → verb → the first position that
// requires it.
type requirement map[string]map[string]string

func (r requirement) add(key, pos string, verbs ...string) {
	if r[key] == nil {
		r[key] = map[string]string{}
	}
	for _, v := range verbs {
		if _, ok := r[key][v]; !ok {
			r[key][v] = pos
		}
	}
}

// required returns what the code controller c's command reaches requires,
// and the sorted package of each WatchesMetadata call it reaches.
func (s *scan) required(t *testing.T, c string) (requirement, []string) {
	t.Helper()
	main := s.mains[c]
	require.NotNil(t, main, c)
	res := rta.Analyze([]*ssa.Function{main.Func("main"), main.Func("init")}, true)
	w := &walker{scan: s, t: t, cg: res.CallGraph, req: requirement{}}
	for fn := range res.Reachable {
		if !w.inModule(fn) {
			continue
		}
		for _, b := range fn.Blocks {
			for _, instr := range b.Instrs {
				if call, ok := instr.(ssa.CallInstruction); ok {
					w.call(call)
				}
				w.libraryRefs(instr)
			}
		}
	}
	slices.Sort(w.metadata)
	return w.req, w.metadata
}

// walker reads one controller's reachable code.
type walker struct {
	*scan
	t        *testing.T
	cg       *callgraph.Graph
	req      requirement
	metadata []string
}

func (w *walker) inModule(fn *ssa.Function) bool {
	pos := w.prog.Fset.Position(fn.Pos())
	if !pos.IsValid() || strings.HasSuffix(pos.Filename, "_test.go") {
		return false
	}
	abs := moduleDir(w.t)
	for _, dir := range []string{"cmd", "internal"} {
		if strings.HasPrefix(pos.Filename, filepath.Join(abs, dir)+string(filepath.Separator)) {
			return true
		}
	}
	return false
}

// libraryRefs records what each library function in libraryGrants that
// instr calls or takes as a value requires.
func (w *walker) libraryRefs(instr ssa.Instruction) {
	for _, op := range instr.Operands(nil) {
		f, ok := (*op).(*ssa.Function)
		if !ok || f.Pkg == nil {
			continue
		}
		for _, key := range libraryGrants[f.Pkg.Pkg.Path()+"."+f.Name()] {
			w.req.add(key, w.pos(instr), "create")
		}
	}
}

func (w *walker) pos(instr ssa.Instruction) string {
	return w.prog.Fset.Position(instr.Pos()).String()
}

// call records what one call site requires.
func (w *walker) call(call ssa.CallInstruction) {
	common := call.Common()
	pos := w.pos(call)
	if common.IsInvoke() {
		m := common.Method
		if m.Pkg() == nil {
			return
		}
		switch path := m.Pkg().Path(); {
		case path == clientPkg:
			verb, ok := clientVerbs[m.Name()]
			if ok {
				obj := common.Args[1]
				if m.Name() == "Get" {
					obj = common.Args[2]
				}
				sub := w.subresource(common.Value, pos)
				w.object(obj, pos, sub, verb)
			}
			if m.Name() == "IndexField" {
				w.object(common.Args[1], pos, "", "list", "watch")
			}
		case strings.HasPrefix(path, "sigs.k8s.io/controller-runtime/"):
			switch m.Name() {
			case "GetEventRecorder":
				w.req.add("events.k8s.io/events", pos, "create", "patch")
			case "GetEventRecorderFor":
				w.req.add("/events", pos, "create", "patch")
			case "GetAPIReader":
				w.t.Errorf("%s: an uncached reader is not modelled", pos)
			}
		}
		return
	}
	callee := common.StaticCallee()
	if callee == nil {
		return
	}
	orig := callee
	if o := callee.Origin(); o != nil {
		orig = o
	}
	if orig.Pkg == nil {
		return
	}
	switch orig.Pkg.Pkg.Path() {
	case controllerutilPkg:
		switch orig.Name() {
		case "CreateOrUpdate":
			w.object(common.Args[2], pos, "", "get", "create", "update")
		case "SetControllerReference":
			w.object(common.Args[0], pos, "finalizers", "update")
		case "CreateOrPatch":
			w.t.Errorf("%s: CreateOrPatch is not modelled", pos)
		}
	case builderPkg:
		switch orig.Name() {
		case "For", "Owns", "Watches":
			w.object(common.Args[1], pos, "", "list", "watch")
		case "WatchesMetadata":
			w.object(common.Args[1], pos, "", "list", "watch")
			w.metadata = append(w.metadata, sitePackage(w.t, pos))
		}
	case unstructuredPkg:
		if orig.Name() == "SetGroupVersionKind" {
			w.gvkArg(common.Args[1], pos)
		}
	}
}

// gvkArg requires a GroupVersionKind set on an unstructured object to be a
// package-level variable in unstructuredKinds, or another unstructured
// object's.
func (w *walker) gvkArg(v ssa.Value, pos string) {
	switch v := v.(type) {
	case *ssa.UnOp:
		if g, ok := v.X.(*ssa.Global); ok {
			_, known := unstructuredKinds[g.Pkg.Pkg.Path()+"."+g.Name()]
			require.True(w.t, known, "%s sets %s, which unstructuredKinds lacks", pos, g.Name())
			return
		}
	case *ssa.Call:
		if f := v.Common().StaticCallee(); f != nil && f.Name() == "GroupVersionKind" && f.Pkg != nil && f.Pkg.Pkg.Path() == unstructuredPkg {
			return
		}
	}
	w.t.Errorf("%s: an unstructured GroupVersionKind not read from unstructuredKinds", pos)
}

// subresource names the subresource a client method is invoked on: "" for
// the client itself, "status" through Status(), the constant argument of
// SubResource().
func (w *walker) subresource(v ssa.Value, pos string) string {
	call, ok := v.(*ssa.Call)
	if !ok || !call.Common().IsInvoke() || call.Common().Method.Pkg().Path() != clientPkg {
		return ""
	}
	switch call.Common().Method.Name() {
	case "Status":
		return "status"
	case "SubResource":
		if c, ok := call.Common().Args[0].(*ssa.Const); ok {
			return constant.StringVal(c.Value)
		}
		w.t.Errorf("%s: SubResource with a name that is not a constant", pos)
	}
	return ""
}

// object requires verbs on every resource v may be, or on its subresource
// sub.
func (w *walker) object(v ssa.Value, pos, sub string, verbs ...string) {
	typs := w.resolve(v, pos, map[ssa.Value]bool{})
	if len(typs) == 0 {
		w.t.Errorf("%s: no object type reaches %s", pos, v)
	}
	for _, typ := range typs {
		keys, cached := w.resources(typ, pos)
		vs := verbs
		if cached && sub == "" && slices.ContainsFunc(verbs, func(v string) bool { return v == "get" || v == "list" }) {
			vs = append(slices.Clone(verbs), "list", "watch")
		}
		for _, key := range keys {
			if sub != "" {
				key += "/" + sub
			}
			w.req.add(key, pos, vs...)
		}
	}
}

// resources returns the "group/resource" keys typ names and whether the
// client caches it.
func (w *walker) resources(typ types.Type, pos string) ([]string, bool) {
	if p, ok := typ.Underlying().(*types.Pointer); ok {
		typ = p.Elem()
	}
	named, ok := types.Unalias(typ).(*types.Named)
	require.True(w.t, ok, "%s: object of unnamed type %s", pos, typ)
	name := named.Obj().Pkg().Path() + "." + named.Obj().Name()
	if named.Obj().Pkg().Path() == unstructuredPkg {
		var keys []string
		for _, k := range unstructuredKinds {
			keys = append(keys, k)
		}
		return keys, false
	}
	gvk, ok := w.kinds[name]
	require.True(w.t, ok, "%s: %s is in no controller's scheme", pos, name)
	plural, ok := w.plurals[gvk.GroupKind()]
	if !ok {
		gvr, _ := meta.UnsafeGuessKindToResource(gvk)
		plural = gvr.Resource
	}
	return []string{gvk.Group + "/" + plural}, !w.uncached[name]
}

// resolve returns the concrete types v may hold.
func (w *walker) resolve(v ssa.Value, pos string, seen map[ssa.Value]bool) []types.Type {
	if seen[v] {
		return nil
	}
	seen[v] = true
	if !types.IsInterface(v.Type()) {
		return []types.Type{v.Type()}
	}
	var out []types.Type
	sources := 0
	add := func(vs ...ssa.Value) {
		sources += len(vs)
		for _, x := range vs {
			out = append(out, w.resolve(x, pos, seen)...)
		}
	}
	switch v := v.(type) {
	case *ssa.MakeInterface:
		add(v.X)
	case *ssa.ChangeInterface:
		add(v.X)
	case *ssa.TypeAssert:
		add(v.X)
	case *ssa.Phi:
		add(v.Edges...)
	case *ssa.Parameter:
		add(w.arguments(v)...)
	case *ssa.FreeVar:
		add(w.bindings(v)...)
	case *ssa.Call:
		if c := v.Common(); c.IsInvoke() && c.Method.Name() == "DeepCopyObject" {
			add(c.Value)
			break
		}
		add(w.results(v.Common(), v, 0)...)
	case *ssa.Extract:
		switch x := v.Tuple.(type) {
		case *ssa.Call:
			add(w.results(x.Common(), x, v.Index)...)
		case *ssa.TypeAssert:
			add(x.X)
		}
	case *ssa.Field:
		add(w.fieldValues(v.X, v.Field)...)
	case *ssa.UnOp:
		if v.Op == token.MUL {
			add(w.loads(v.X, map[ssa.Value]bool{})...)
		}
	case *ssa.Index:
		add(w.elems(v.X, map[ssa.Value]bool{})...)
	case *ssa.Const:
		return nil
	}
	if sources == 0 {
		w.t.Errorf("%s: cannot tell what %s (%T at %s) holds", pos, v.Name(), v, w.prog.Fset.Position(v.Pos()))
	}
	return out
}

// elems returns what may be stored in the elements of slice or array v.
func (w *walker) elems(v ssa.Value, seen map[ssa.Value]bool) []ssa.Value {
	v = arrayBase(v)
	if seen[v] {
		return nil
	}
	seen[v] = true
	var out []ssa.Value
	add := func(vs ...ssa.Value) {
		for _, x := range vs {
			out = append(out, w.elems(x, seen)...)
		}
	}
	switch v := v.(type) {
	case *ssa.Alloc:
		out = append(out, w.allocs[v]...)
	case *ssa.Phi:
		add(v.Edges...)
	case *ssa.Parameter:
		add(w.arguments(v)...)
	case *ssa.FreeVar:
		add(w.bindings(v)...)
	case *ssa.Call:
		if b, ok := v.Common().Value.(*ssa.Builtin); ok && b.Name() == "append" {
			add(v.Common().Args...)
		}
	case *ssa.UnOp:
		if v.Op == token.MUL {
			add(w.loads(v.X, map[ssa.Value]bool{})...)
		}
	}
	return out
}

// loads returns what may be stored at address addr.
func (w *walker) loads(addr ssa.Value, seen map[ssa.Value]bool) []ssa.Value {
	if seen[addr] {
		return nil
	}
	seen[addr] = true
	var out []ssa.Value
	add := func(vs ...ssa.Value) {
		for _, x := range vs {
			out = append(out, w.loads(x, seen)...)
		}
	}
	switch a := addr.(type) {
	case *ssa.FieldAddr:
		out = append(out, w.fieldValues(a.X, a.Field)...)
	case *ssa.IndexAddr:
		out = append(out, w.elems(a.X, map[ssa.Value]bool{})...)
	case *ssa.Global:
		out = append(out, w.globals[a]...)
	case *ssa.Alloc:
		out = append(out, w.allocs[a]...)
	case *ssa.FreeVar:
		add(w.bindings(a)...)
	case *ssa.Parameter:
		add(w.arguments(a)...)
	case *ssa.Phi:
		add(a.Edges...)
	}
	return out
}

// arguments returns what every call reaching p's function passes for p.
func (w *walker) arguments(p *ssa.Parameter) []ssa.Value {
	fn := p.Parent()
	i := slices.Index(fn.Params, p)
	node := w.cg.Nodes[fn]
	if node == nil {
		return nil
	}

	var out []ssa.Value
	for _, e := range node.In {
		c := e.Site.Common()
		switch {
		case c.IsInvoke() && i == 0:
			out = append(out, c.Value)
		case c.IsInvoke():
			out = append(out, c.Args[i-1])
		case i < len(c.Args):
			out = append(out, c.Args[i])
		}
	}
	return out
}

// bindings returns what every closure of v's function binds to v.
func (w *walker) bindings(v *ssa.FreeVar) []ssa.Value {
	fn := v.Parent()
	i := slices.Index(fn.FreeVars, v)
	var out []ssa.Value
	for _, mc := range w.closures[fn] {
		out = append(out, mc.Bindings[i])
	}
	return out
}

// results returns result i of every function call may reach.
func (w *walker) results(c *ssa.CallCommon, site ssa.CallInstruction, i int) []ssa.Value {
	var callees []*ssa.Function
	if f := c.StaticCallee(); f != nil {
		callees = append(callees, f)
	} else if !c.IsInvoke() {
		callees = w.funcs(c.Value, map[ssa.Value]bool{})
	} else if node := w.cg.Nodes[site.Parent()]; node != nil {
		for _, e := range node.Out {
			if e.Site == site {
				callees = append(callees, e.Callee.Func)
			}
		}
	}
	var out []ssa.Value
	for _, f := range callees {
		for _, b := range f.Blocks {
			if ret, ok := b.Instrs[len(b.Instrs)-1].(*ssa.Return); ok && i < len(ret.Results) {
				out = append(out, ret.Results[i])
			}
		}
	}
	return out
}

// funcs returns the functions func value v may be.
func (w *walker) funcs(v ssa.Value, seen map[ssa.Value]bool) []*ssa.Function {
	if seen[v] {
		return nil
	}
	seen[v] = true
	var out []*ssa.Function
	add := func(vs ...ssa.Value) {
		for _, x := range vs {
			out = append(out, w.funcs(x, seen)...)
		}
	}
	switch v := v.(type) {
	case *ssa.Function:
		out = append(out, v)
	case *ssa.MakeClosure:
		out = append(out, v.Fn.(*ssa.Function))
	case *ssa.UnOp:
		if v.Op == token.MUL {
			add(w.loads(v.X, map[ssa.Value]bool{})...)
		}
	case *ssa.Field:
		add(w.fieldValues(v.X, v.Field)...)
	case *ssa.Parameter:
		add(w.arguments(v)...)
	case *ssa.FreeVar:
		add(w.bindings(v)...)
	case *ssa.Phi:
		add(v.Edges...)
	}
	return out
}

// fieldValues returns what may be stored in field i of base, a struct or a
// pointer to one; where base's roots are unknown, that is every store into
// that field of its type.
func (w *walker) fieldValues(base ssa.Value, i int) []ssa.Value {
	roots, ok := w.roots(base, map[ssa.Value]bool{})
	var out []ssa.Value
	for _, r := range roots {
		out = append(out, w.located[fieldLoc{r, i}]...)
	}
	if !ok || len(out) == 0 {
		return w.fields[fieldKey(base.Type(), i)]
	}
	return out
}

// roots returns the Allocs and Globals struct or pointer v may come from,
// and false where some source is not one this follows.
func (w *walker) roots(v ssa.Value, seen map[ssa.Value]bool) ([]ssa.Value, bool) {
	if seen[v] {
		return nil, true
	}
	seen[v] = true
	var out []ssa.Value
	ok := true
	add := func(vs ...ssa.Value) {
		for _, x := range vs {
			rs, k := w.roots(x, seen)
			out = append(out, rs...)
			ok = ok && k
		}
	}
	switch v := v.(type) {
	case *ssa.Alloc:
		out = append(out, v)
		add(w.allocs[v]...)
	case *ssa.Global:
		out = append(out, v)
		add(w.globals[v]...)
	case *ssa.IndexAddr:
		add(w.sliceRoots(v.X)...)
	case *ssa.Slice:
		add(v.X)
	case *ssa.UnOp:
		if v.Op != token.MUL {
			return nil, false
		}
		add(v.X)
		add(w.loads(v.X, map[ssa.Value]bool{})...)
	case *ssa.Parameter:
		add(w.arguments(v)...)
	case *ssa.FreeVar:
		add(w.bindings(v)...)
	case *ssa.Phi:
		add(v.Edges...)
	default:
		return nil, false
	}
	return out, ok
}

// sliceRoots returns the arrays slice v may share.
func (w *walker) sliceRoots(v ssa.Value) []ssa.Value {
	switch v := v.(type) {
	case *ssa.UnOp:
		if g, ok := v.X.(*ssa.Global); ok {
			return w.globals[g]
		}
	case *ssa.Call:
		if b, ok := v.Common().Value.(*ssa.Builtin); ok && b.Name() == "append" {
			return v.Common().Args
		}
	}
	return []ssa.Value{v}
}
