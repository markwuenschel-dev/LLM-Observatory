import os
from pathlib import Path
import subprocess
import tempfile
import unittest

from observatory.project import resolve_project, sanitize_remote


def run_git(path: Path, *args: str) -> str:
    result = subprocess.run(["git", "-C", str(path), *args], check=True, capture_output=True, text=True)
    return result.stdout.strip()


class ProjectResolutionTests(unittest.TestCase):
    def test_sanitize_remote_removes_credentials_and_query(self) -> None:
        self.assertEqual(
            sanitize_remote("https://alice:secret@example.com/acme/demo.git?token=bad"),
            "https://example.com/acme/demo.git",
        )
        self.assertEqual(sanitize_remote("git@example.com:acme/demo.git"), "ssh://example.com/acme/demo.git")
        self.assertIsNone(sanitize_remote("C:\\private\\repo"))

    def test_resolves_repository_from_subdirectory(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            run_git(root, "init", "-b", "main")
            run_git(root, "config", "user.email", "observatory@example.invalid")
            run_git(root, "config", "user.name", "Observatory Test")
            (root / "README.md").write_text("fixture\n", encoding="utf-8")
            run_git(root, "add", "README.md")
            run_git(root, "commit", "-m", "fixture")
            run_git(root, "remote", "add", "origin", "https://alice:secret@example.com/acme/demo.git?x=1")
            nested = root / "src" / "nested"
            nested.mkdir(parents=True)

            identity = resolve_project(nested)

            self.assertEqual(Path(identity.root), root.resolve())
            self.assertEqual(identity.repository, "demo")
            self.assertEqual(identity.remote, "https://example.com/acme/demo.git")
            self.assertEqual(identity.branch, "main")
            self.assertEqual(len(identity.commit or ""), 40)
            self.assertTrue(identity.project_id.startswith("repo_sha256:"))
            self.assertTrue(identity.worktree.startswith("worktree_sha256:"))
            self.assertNotIn("secret", repr(identity))

    def test_no_commit_and_non_repository_have_explicit_fallbacks(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            run_git(root, "init", "-b", "main")
            run_git(root, "remote", "add", "origin", "git@example.com:acme/empty.git")
            pending = resolve_project(root)
            self.assertIsNone(pending.commit)
            self.assertEqual(pending.remote, "ssh://example.com/acme/empty.git")

            with tempfile.TemporaryDirectory() as non_repo_temp:
                non_repo = Path(non_repo_temp)
                fallback = resolve_project(non_repo)
            self.assertTrue(fallback.project_id.startswith("local_sha256:"))
            self.assertIsNone(fallback.repository)
            self.assertEqual(Path(fallback.root), non_repo.resolve())


if __name__ == "__main__":
    unittest.main()
