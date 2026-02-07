const usernameInput = document.getElementById('username-input');
const passwordInput = document.getElementById('password-input');
const apiKeyInput = document.getElementById('api-key-input');

function buildCreds() {
  if (passwordInput) {
    const username = (usernameInput ? usernameInput.value : '').trim() || 'admin';
    const password = passwordInput.value.trim();
    return { username, password };
  }

  const password = (apiKeyInput ? apiKeyInput.value : '').trim();
  return { username: 'admin', password };
}

async function requestLogin(creds) {
  const res = await fetch('/api/v1/admin/login', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(creds),
  });
  return res.ok;
}

function bindEnter(el) {
  if (!el) return;
  el.addEventListener('keypress', (e) => {
    if (e.key === 'Enter') login();
  });
}

bindEnter(usernameInput);
bindEnter(passwordInput);
bindEnter(apiKeyInput);

async function login() {
  const creds = buildCreds();
  if (!creds.password) return;

  try {
    const ok = await requestLogin(creds);
    if (ok) {
      await storeAppKey(creds);
      window.location.href = '/admin/token';
    } else {
      showToast('Invalid username or password', 'error');
    }
  } catch (e) {
    showToast('Connection failed', 'error');
  }
}

(async () => {
  const existingCreds = await getStoredAppKey();
  if (!existingCreds || !existingCreds.password) return;
  try {
    const ok = await requestLogin(existingCreds);
    if (ok) window.location.href = '/admin/token';
  } catch (e) {
    return;
  }
})();
