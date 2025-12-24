# Security & Secret Management

This document outlines the security measures implemented to prevent secret leaks and ensure production-grade security practices.

## 🔒 Secret Leak Prevention

### 1. Startup Validation

The application performs defensive validation at startup to detect misconfigured secrets:

**Location:** [`api/main.py`](file:///f:/kasparro-etl/api/main.py)

```python
# Validate API Key configuration (defensive engineering)
if not settings.API_KEY or settings.API_KEY in ("", "YOUR_API_KEY_HERE"):
    logger.warning("⚠️  API_KEY not set or using placeholder value")
    logger.warning("   Set API_KEY in .env file for production use")
else:
    logger.info("✓ API_KEY configured")
```

**Benefits:**
- Warns developers immediately if secrets are missing
- Prevents silent failures in production
- Demonstrates defensive engineering practices

---

### 2. Pre-commit Hook

A Python script scans staged files for potential secrets before allowing commits.

**Location:** [`scripts/check_secrets.py`](file:///f:/kasparro-etl/scripts/check_secrets.py)

**Detects:**
- CoinGecko API key patterns (`CG-...`)
- Real API key values (not placeholders)
- OpenAI-style secret keys (`sk-...`)
- Hardcoded passwords
- Secret key patterns

**Installation:**

```bash
# Option 1: Manual pre-commit hook
cp scripts/check_secrets.py .git/hooks/pre-commit
chmod +x .git/hooks/pre-commit

# Option 2: Run manually before commits
python scripts/check_secrets.py
```

**Example Output:**

```
🔍 Scanning for secrets in staged files...
✓ No secrets detected - commit allowed
```

Or if secrets found:

```
❌ SECRET LEAK DETECTED - COMMIT BLOCKED

File: .env.example:5
Issue: Real API key value detected
Match: API_KEY=CG-J3Aeeda7AUTui6Ji257F2adh...

🔒 SECURITY VIOLATION - Please remove secrets before committing
```

---

### 3. CI/CD Secret Detection

GitHub Actions workflow automatically scans for secrets on every push and pull request.

**Location:** [`.github/workflows/security-check.yml`](file:///f:/kasparro-etl/.github/workflows/security-check.yml)

**Checks:**
1. Runs `check_secrets.py` on all files
2. Verifies `.env` is gitignored
3. Checks `.env.example` for real API keys
4. Fails CI build if secrets detected

**Workflow triggers:**
- Every push to `main` or `develop`
- Every pull request
- Manual workflow dispatch

---

## 🛡️ Security Best Practices

### Implemented Measures

| Measure | Status | Description |
|---------|--------|-------------|
| **No Secrets in Repo** | ✅ | All secrets excluded via `.gitignore` |
| **Placeholder Templates** | ✅ | `.env.example` contains safe placeholders |
| **Runtime Injection** | ✅ | Secrets loaded from environment variables |
| **Startup Validation** | ✅ | Warns if secrets misconfigured |
| **Pre-commit Hook** | ✅ | Blocks commits with secrets |
| **CI/CD Scanning** | ✅ | Automated secret detection in pipeline |

### Defense in Depth

```
Layer 1: .gitignore          → Prevents .env from being tracked
Layer 2: Pre-commit Hook     → Scans before commit
Layer 3: CI/CD Pipeline      → Automated scanning on push
Layer 4: Startup Validation  → Runtime configuration check
```

---

## 📋 Developer Workflow

### Setting Up Locally

1. **Copy template:**
   ```bash
   cp .env.example .env
   ```

2. **Add real secrets:**
   ```env
   API_KEY=your_real_coingecko_api_key_here
   ```

3. **Install pre-commit hook (optional but recommended):**
   ```bash
   cp scripts/check_secrets.py .git/hooks/pre-commit
   chmod +x .git/hooks/pre-commit
   ```

4. **Verify configuration:**
   ```bash
   # Check .env is gitignored
   git check-ignore .env
   
   # Scan for secrets manually
   python scripts/check_secrets.py
   ```

### Before Committing

The pre-commit hook will automatically run, but you can also run manually:

```bash
python scripts/check_secrets.py
```

---

## 🚨 What to Do If Secrets Are Committed

If you accidentally commit secrets:

1. **Immediately rotate the secret** (get a new API key)
2. **Remove from git history:**
   ```bash
   git filter-branch --force --index-filter \
     "git rm --cached --ignore-unmatch .env" \
     --prune-empty --tag-name-filter cat -- --all
   ```
3. **Force push** (if already pushed):
   ```bash
   git push origin --force --all
   ```
4. **Verify secret is gone:**
   ```bash
   git log --all --full-history -- .env
   ```

---

## 🎯 Production Deployment

### Cloud Secret Management

**AWS:**
- Use AWS Secrets Manager or Parameter Store
- Inject via EC2 user data or ECS task definitions

**GCP:**
- Use Secret Manager
- Inject via instance metadata or Cloud Run

**Docker:**
- Use `.env` file (not committed)
- Or pass via `docker run -e API_KEY=...`

### Example: AWS Secrets Manager

```bash
# Store secret
aws secretsmanager create-secret \
  --name kasparro-etl/api-key \
  --secret-string "your-real-api-key"

# Retrieve in application
API_KEY=$(aws secretsmanager get-secret-value \
  --secret-id kasparro-etl/api-key \
  --query SecretString --output text)
```

---

## ✅ Verification

Run these commands to verify security measures:

```bash
# 1. Check .env is gitignored
git check-ignore .env

# 2. Scan for secrets
python scripts/check_secrets.py

# 3. Verify no secrets in .env.example
grep -E "CG-[A-Za-z0-9]{20,}" .env.example || echo "✓ No real keys"

# 4. Check git history for secrets
git log --all --full-history -- .env
```

**Expected Results:**
- `.env` is gitignored ✓
- No secrets detected ✓
- `.env.example` has placeholders only ✓
- No `.env` in git history ✓

---

## 📚 Additional Resources

- [OWASP Secrets Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html)
- [GitHub Secret Scanning](https://docs.github.com/en/code-security/secret-scanning)
- [AWS Secrets Manager Best Practices](https://docs.aws.amazon.com/secretsmanager/latest/userguide/best-practices.html)

---

**Security is a continuous process. Stay vigilant! 🔒**
