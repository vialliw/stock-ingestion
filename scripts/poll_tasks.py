"""Polling script for any agent — pass role as argument.

Usage:
    python poll_tasks.py B    # for Agent B (Executor)
    python poll_tasks.py C    # for Agent C (Challenger)
    python poll_tasks.py D    # for Agent D (Critic)
"""
import json
import sys

role = sys.argv[1] if len(sys.argv) > 1 else "B"

tasks = json.load(open('devin_blackboard/TASKS.json'))
done_ids = {t['id'] for t in tasks if t['status'] == 'done'}

found = False
for t in tasks:
    if t.get('assigned_to') == role and t['status'] == 'todo':
        deps = t.get('depends_on', [])
        deps_ok = all(d in done_ids for d in deps)
        status_str = 'READY' if deps_ok else 'WAITING'
        print(f'{status_str}: {t["id"]} assigned_to={t["assigned_to"]} deps={deps} deps_ok={deps_ok}')
        if deps_ok:
            print(f'  title: {t["title"]}')
            print(f'  input_files: {t.get("input_files", [])}')
            print(f'  output_files: {t.get("output_files", [])}')
            print(f'  acceptance_criteria: {t.get("acceptance_criteria", "")[:200]}...')
        found = True

if not found:
    print(f'No tasks assigned_to={role} with status=todo.')

print(f'\n--- Summary ---')
print(f'done_ids count: {len(done_ids)}')
for t in tasks:
    if t['status'] == 'todo':
        print(f'TODO: {t["id"]} assigned_to={t["assigned_to"]} role={t["role"]}')
