#!/usr/bin/env python3
"""
Pre-commit hook to prevent secrets from being committed to the repository.

This script scans staged files for potential secrets and fails the commit if found.
Install: Copy to .git/hooks/pre-commit and make executable (chmod +x)
"""

import re
import sys
import subprocess
from pathlib import Path


# Patterns that indicate potential secrets
SECRET_PATTERNS = [
    (r'CG-[A-Za-z0-9]{20,}', 'CoinGecko API key pattern detected'),
    (r'API_KEY\s*=\s*["\']?(?!YOUR_API_KEY_HERE|your_api_key_here|<|{{)[A-Za-z0-9_-]{20,}', 'Real API key value detected'),
    (r'sk-[A-Za-z0-9]{32,}', 'OpenAI-style secret key detected'),
    (r'(?i)(password|passwd|pwd)\s*=\s*["\'][^"\']{8,}["\']', 'Hardcoded password detected'),
    (r'(?i)secret[_-]?key\s*=\s*["\'][^"\']{16,}["\']', 'Secret key detected'),
]

# Files to always check
ALWAYS_CHECK = ['.env.example', 'README.md', 'ARCHITECTURE.md']

# Files to skip (contains intentional example secrets for documentation)
SKIP_FILES = ['SECURITY.md', 'COMMIT_MESSAGE.txt']

# Files to skip by pattern
SKIP_PATTERNS = [
    r'\.git/',
    r'\.venv/',
    r'__pycache__/',
    r'\.pyc$',
    r'\.log$',
    r'\.db$',
    r'\.sqlite',
    r'node_modules/',
]


def get_staged_files():
    """Get list of staged files"""
    try:
        result = subprocess.run(
            ['git', 'diff', '--cached', '--name-only', '--diff-filter=ACM'],
            capture_output=True,
            text=True,
            check=True
        )
        return result.stdout.strip().split('\n') if result.stdout.strip() else []
    except subprocess.CalledProcessError:
        return []


def should_skip_file(filepath):
    """Check if file should be skipped"""
    # Check if file is in skip list
    from pathlib import Path
    filename = Path(filepath).name
    if filename in SKIP_FILES or filepath in SKIP_FILES:
        return True
    
    # Check patterns
    for pattern in SKIP_PATTERNS:
        if re.search(pattern, filepath):
            return True
    return False


def scan_file(filepath):
    """Scan a file for potential secrets"""
    violations = []
    
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
            
        for pattern, message in SECRET_PATTERNS:
            matches = re.finditer(pattern, content, re.MULTILINE)
            for match in matches:
                # Get line number
                line_num = content[:match.start()].count('\n') + 1
                violations.append({
                    'file': filepath,
                    'line': line_num,
                    'message': message,
                    'match': match.group(0)[:50]  # First 50 chars
                })
    except Exception as e:
        print(f"Warning: Could not scan {filepath}: {e}", file=sys.stderr)
    
    return violations


def main():
    """Main pre-commit hook logic"""
    print("🔍 Scanning for secrets in staged files...")
    
    staged_files = get_staged_files()
    if not staged_files:
        print("✓ No files to check")
        return 0
    
    all_violations = []
    
    for filepath in staged_files:
        # Skip non-existent files (deleted files)
        if not Path(filepath).exists():
            continue
            
        # Skip certain file types
        if should_skip_file(filepath):
            continue
        
        # Scan file
        violations = scan_file(filepath)
        all_violations.extend(violations)
    
    # Report violations
    if all_violations:
        print("\n" + "="*70)
        print("❌ SECRET LEAK DETECTED - COMMIT BLOCKED")
        print("="*70)
        print("\nThe following potential secrets were found:\n")
        
        for v in all_violations:
            print(f"  File: {v['file']}:{v['line']}")
            print(f"  Issue: {v['message']}")
            print(f"  Match: {v['match']}...")
            print()
        
        print("="*70)
        print("🔒 SECURITY VIOLATION - Please remove secrets before committing")
        print("="*70)
        print("\nTo fix:")
        print("  1. Replace real secrets with placeholders (e.g., YOUR_API_KEY_HERE)")
        print("  2. Ensure .env file is gitignored (not .env.example)")
        print("  3. Use environment variables for real secrets")
        print()
        
        return 1
    
    print("✓ No secrets detected - commit allowed")
    return 0


if __name__ == '__main__':
    sys.exit(main())
