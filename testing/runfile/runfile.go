// Package runfile resolves paths to test-fixture data.
//
// Under `bazel test` the fixtures are provided as runfiles and located via the
// rules_go bazel helper. Under plain `go test` there are no runfiles, so the
// fixtures are instead extracted by CI into a single directory pointed to by the
// WF_TESTDATA_DIR environment variable, laid out with the same relative paths the
// tests already request (e.g. "mainnet/...", "external/eth2_networks", "sign").
//
// Path/Walk look under WF_TESTDATA_DIR first and fall back to bazel runfiles, so
// the tests keep working under both `go test` and `bazel test`.
package runfile

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/bazelbuild/rules_go/go/tools/bazel"
)

// EnvDir is the environment variable naming the extracted test-data root used
// under `go test`. It is empty under `bazel test`, where runfiles are used.
const EnvDir = "WF_TESTDATA_DIR"

// Path returns an absolute path to the fixture named by p. It first looks under
// $WF_TESTDATA_DIR and falls back to the bazel runfiles lookup.
func Path(p string) (string, error) {
	if root := os.Getenv(EnvDir); root != "" {
		fp := filepath.Join(root, p)
		if _, err := os.Stat(fp); err == nil {
			return fp, nil
		}
		// Report the miss with the same wording bazel.Runfile uses, because some
		// callers sniff for "could not locate file" to tell an intentionally-absent
		// fixture (a negative test case) from a hard error. Do not fall back to
		// bazel.Runfile here: under `go test` it would instead report "could not
		// locate runfiles directory", which those callers treat as fatal.
		return "", fmt.Errorf("Runfile %s: could not locate file", p)
	}
	return bazel.Runfile(p)
}

// Walk returns the absolute paths of all regular files under
// $WF_TESTDATA_DIR/prefix. It reports whether WF_TESTDATA_DIR is set: when it is
// not, the caller should fall back to a bazel runfiles enumeration.
func Walk(prefix string) (paths []string, ok bool, err error) {
	root := os.Getenv(EnvDir)
	if root == "" {
		return nil, false, nil
	}
	base := filepath.Join(root, prefix)
	err = filepath.Walk(base, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			paths = append(paths, p)
		}
		return nil
	})
	return paths, true, err
}
