"""Fetch the human requester for Copilot-authored PRs via GraphQL ASSIGNED_EVENT timeline."""
import sqlite3
import subprocess
import json
import time
import sys

DB_PATH = "pr-dashboard.db"
BATCH_SIZE = 50
BOT_AUTHORS = ("Copilot", "copilot-swe-agent[bot]")
BOT_LOGINS = {"copilot-swe-agent", "copilot-swe-agent[bot]", "Copilot", "app/copilot-swe-agent"}

def run_graphql(query):
    result = subprocess.run(
        ["gh", "api", "graphql", "-f", f"query={query}"],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        print(f"  GraphQL error: {result.stderr.strip()}", file=sys.stderr)
        return None
    return json.loads(result.stdout)

def main():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("ALTER TABLE items ADD COLUMN copilot_requester TEXT")
    conn.commit()
    print("Added copilot_requester column")

    # Find all Copilot-authored PRs
    rows = conn.execute(
        "SELECT repo, number FROM items WHERE is_pull_request=1 AND author IN (?,?) ORDER BY repo, number",
        BOT_AUTHORS
    ).fetchall()
    print(f"Found {len(rows)} Copilot-authored PRs")

    # Group by repo
    by_repo = {}
    for repo, number in rows:
        by_repo.setdefault(repo, []).append(number)

    total_found = 0
    for repo, numbers in sorted(by_repo.items()):
        owner, name = repo.split("/")
        print(f"\n{repo}: {len(numbers)} PRs")

        for batch_start in range(0, len(numbers), BATCH_SIZE):
            batch = numbers[batch_start:batch_start + BATCH_SIZE]
            parts = []
            for i, num in enumerate(batch):
                parts.append(
                    f'pr{i}: pullRequest(number:{num}) {{ '
                    f'number '
                    f'timelineItems(first:5,itemTypes:ASSIGNED_EVENT) {{ '
                    f'nodes {{ ... on AssignedEvent {{ actor{{login}} assignee{{...on User{{login}}...on Bot{{login}}}} }} }} '
                    f'}} }}'
                )
            query = f'{{ repository(owner:"{owner}",name:"{name}") {{ {" ".join(parts)} }} }}'
            
            data = run_graphql(query)
            if not data or "data" not in data:
                print(f"  Batch {batch_start//BATCH_SIZE + 1} failed, retrying in 5s...")
                time.sleep(5)
                data = run_graphql(query)
                if not data or "data" not in data:
                    print(f"  Batch {batch_start//BATCH_SIZE + 1} failed again, skipping")
                    continue

            repo_data = data["data"]["repository"]
            batch_found = 0
            for i, num in enumerate(batch):
                pr_data = repo_data.get(f"pr{i}")
                if not pr_data or not pr_data.get("timelineItems"):
                    continue
                
                # Find assignee that was assigned by copilot-swe-agent
                for node in pr_data["timelineItems"]["nodes"]:
                    actor = (node.get("actor") or {}).get("login", "")
                    assignee = (node.get("assignee") or {}).get("login", "")
                    if actor.lower() in {b.lower() for b in BOT_LOGINS} and assignee and assignee not in BOT_LOGINS:
                        conn.execute(
                            "UPDATE items SET copilot_requester=? WHERE repo=? AND number=?",
                            (assignee, repo, num)
                        )
                        batch_found += 1
                        break
                else:
                    # Fallback: any non-bot assignee
                    for node in pr_data["timelineItems"]["nodes"]:
                        assignee = (node.get("assignee") or {}).get("login", "")
                        if assignee and assignee not in BOT_LOGINS:
                            conn.execute(
                                "UPDATE items SET copilot_requester=? WHERE repo=? AND number=?",
                                (assignee, repo, num)
                            )
                            batch_found += 1
                            break

            conn.commit()
            total_found += batch_found
            done = min(batch_start + BATCH_SIZE, len(numbers))
            print(f"  {done}/{len(numbers)} — found {batch_found} requesters in this batch")

    # Summary
    found = conn.execute("SELECT count(*) FROM items WHERE copilot_requester IS NOT NULL").fetchone()[0]
    total = conn.execute("SELECT count(*) FROM items WHERE author IN (?,?)", BOT_AUTHORS).fetchone()[0]
    print(f"\nDone! Found requester for {found}/{total} Copilot PRs")
    conn.close()

if __name__ == "__main__":
    main()
