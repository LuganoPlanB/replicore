import { execFileSync } from "node:child_process"
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs"
import { tmpdir } from "node:os"
import path from "node:path"

const config = JSON.parse(readFileSync(".github/release-config.json", "utf8"))
const tagPrefix = config.tagPrefix ?? "v"

main()

function main() {
  git("fetch", "origin", "main", "--tags")
  const head = git("rev-parse", "HEAD")
  const originMain = git("rev-parse", "origin/main")
  if (head !== originMain) {
    console.log(`Skipping release for stale tested commit ${head}; origin/main is ${originMain}.`)
    return
  }

  const packageJson = readJson("package.json")
  const latestTag = latestSemverTag()
  const currentVersion = latestTag ? latestTag.slice(tagPrefix.length) : packageJson.version
  const commits = readCommits(latestTag)
  const release = planRelease(currentVersion, commits)

  if (!release) {
    console.log("No releasable conventional commits found.")
    return
  }

  const nextTag = `${tagPrefix}${release.version}`
  if (tagExists(nextTag)) {
    console.log(`Tag ${nextTag} already exists.`)
    return
  }

  if (process.env.RELEASE_DRY_RUN === "1") {
    console.log(JSON.stringify(release, null, 2))
    return
  }

  updatePackageVersion(release.version)
  const changelogEntry = renderChangelogEntry(release)
  updateChangelog(changelogEntry)
  commitRelease(release.version)
  git("tag", "-a", nextTag, "-m", nextTag)
  git("push", "origin", "HEAD:main")
  git("push", "origin", nextTag)
  createGitHubRelease(nextTag, changelogEntry)
}

function readJson(file) {
  return JSON.parse(readFileSync(file, "utf8"))
}

function updatePackageVersion(version) {
  const packageJson = readJson("package.json")
  packageJson.version = version
  writeJson("package.json", packageJson)

  const lockfile = readJson("package-lock.json")
  lockfile.version = version
  if (lockfile.packages?.[""]) lockfile.packages[""].version = version
  writeJson("package-lock.json", lockfile)
}

function writeJson(file, value) {
  writeFileSync(file, `${JSON.stringify(value, null, 2)}\n`)
}

function updateChangelog(entry) {
  const header = "# Changelog\n\n"
  const existing = existsSync("CHANGELOG.md") ? readFileSync("CHANGELOG.md", "utf8") : header
  const body = existing.startsWith(header) ? existing.slice(header.length) : existing
  writeFileSync("CHANGELOG.md", `${header}${entry}\n${body}`)
}

function commitRelease(version) {
  git("add", "CHANGELOG.md", "package.json", "package-lock.json")
  const changed = git("diff", "--cached", "--name-only")
  if (!changed) throw new Error("Release produced no changed files.")
  git("commit", "-m", `chore(release): ${tagPrefix}${version} [skip ci]`)
}

function latestSemverTag() {
  const tags = git("tag", "--list", `${tagPrefix}[0-9]*`, "--sort=-v:refname")
    .split("\n")
    .filter(Boolean)
  return tags.find((tag) => semverPattern().test(tag)) ?? null
}

function tagExists(tag) {
  try {
    git("rev-parse", "-q", "--verify", `refs/tags/${tag}`)
    return true
  } catch {
    return false
  }
}

function readCommits(latestTag) {
  const range = latestTag ? `${latestTag}..HEAD` : `${config.bootstrapSha}..HEAD`
  const raw = git("log", "--format=%H%x00%s%x00%b%x1e", range)
  return raw
    .split("\x1e")
    .map((entry) => entry.trim())
    .filter(Boolean)
    .map((entry) => {
      const [sha, subject, body = ""] = entry.split("\x00")
      return parseCommit(sha, subject, body)
    })
}

function parseCommit(sha, subject, body) {
  const match = subject.match(/^([a-z]+)(\([^)]+\))?(!)?: (.+)$/)
  const breaking = Boolean(match?.[3]) || /(^|\n)BREAKING CHANGE:/.test(body)
  return {
    sha,
    subject,
    type: match?.[1] ?? null,
    scope: match?.[2]?.slice(1, -1) ?? null,
    description: match?.[4] ?? subject,
    breaking
  }
}

function planRelease(currentVersion, commits) {
  const entries = commits.filter((commit) => releaseSection(commit) !== null)
  if (entries.length === 0) return null

  const bump = entries.some((commit) => commit.breaking)
    ? "major"
    : entries.some((commit) => commit.type === "feat")
      ? "minor"
      : "patch"

  return {
    version: bumpVersion(currentVersion, bump),
    date: new Date().toISOString().slice(0, 10),
    bump,
    entries
  }
}

function bumpVersion(version, bump) {
  const match = version.match(/^(\d+)\.(\d+)\.(\d+)$/)
  if (!match) throw new Error(`Unsupported semver version: ${version}`)
  const major = Number(match[1])
  const minor = Number(match[2])
  const patch = Number(match[3])
  if (bump === "major") return `${major + 1}.0.0`
  if (bump === "minor") return `${major}.${minor + 1}.0`
  return `${major}.${minor}.${patch + 1}`
}

function renderChangelogEntry(release) {
  const groups = [
    ["Breaking Changes", release.entries.filter((commit) => commit.breaking)],
    ["Features", release.entries.filter((commit) => !commit.breaking && commit.type === "feat")],
    ["Bug Fixes", release.entries.filter((commit) => !commit.breaking && commit.type === "fix")],
    ["Performance", release.entries.filter((commit) => !commit.breaking && commit.type === "perf")]
  ].filter(([, commits]) => commits.length > 0)

  const lines = [`## ${tagPrefix}${release.version} - ${release.date}`, ""]
  for (const [title, commits] of groups) {
    lines.push(`### ${title}`, "")
    for (const commit of commits) {
      const scope = commit.scope ? `**${commit.scope}:** ` : ""
      lines.push(`- ${scope}${commit.description} (${commit.sha.slice(0, 7)})`)
    }
    lines.push("")
  }
  return lines.join("\n")
}

function releaseSection(commit) {
  if (commit.breaking) return "breaking"
  if (["feat", "fix", "perf"].includes(commit.type)) return commit.type
  return null
}

function createGitHubRelease(tag, notes) {
  const dir = mkdtempSync(path.join(tmpdir(), "replicore-release-"))
  const notesFile = path.join(dir, "notes.md")
  try {
    writeFileSync(notesFile, notes)
    execFileSync("gh", ["release", "create", tag, "--title", tag, "--notes-file", notesFile], {
      stdio: "inherit",
      env: process.env
    })
  } finally {
    rmSync(dir, { recursive: true, force: true })
  }
}

function semverPattern() {
  return new RegExp(`^${escapeRegExp(tagPrefix)}\\d+\\.\\d+\\.\\d+$`)
}

function escapeRegExp(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")
}

function git(...args) {
  return execFileSync("git", args, { encoding: "utf8" }).trim()
}
