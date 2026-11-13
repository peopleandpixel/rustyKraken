#!/bin/bash
# Quality Gates — alle Checks durchlaufen
# Dieses Skript MUSS vor jedem Commit ausgeführt werden.
# cargo fmt ist ein verpflichtender Schritt und darf NICHT übersprungen werden.

set -e

echo "========================================="
echo "  Quality Gates — Alle Checks"
echo "========================================="
echo ""

# 1. Format-Check (cargo fmt) — MUSS als erstes laufen
echo "[1/4] cargo fmt --check ..."
if ! cargo fmt --all --check 2>&1; then
    echo "FEHLER: Code ist nicht korrekt formatiert!"
    echo "Fuehre 'cargo fmt' aus, um die Formatierung zu korrigieren."
    exit 1
fi
echo "  ✓ Format-Check bestanden"
echo ""

# 2. Clippy (Linting)
echo "[2/4] cargo clippy --all-targets -- -D warnings ..."
if ! cargo clippy --all-targets -- -D warnings 2>&1; then
    echo "FEHLER: Clippy hat Warnungen gefunden!"
    exit 1
fi
echo "  ✓ Clippy bestanden"
echo ""

# 3. Build
echo "[3/4] cargo build ..."
if ! cargo build 2>&1; then
    echo "FEHLER: Build fehlgeschlagen!"
    exit 1
fi
echo "  ✓ Build bestanden"
echo ""

# 4. Tests
echo "[4/4] cargo test ..."
if ! cargo test 2>&1; then
    echo "FEHLER: Tests fehlgeschlagen!"
    exit 1
fi
echo "  ✓ Tests bestanden"
echo ""

echo "========================================="
echo "  ALLE QUALITY GATES BESTANDEN ✓"
echo "========================================="
