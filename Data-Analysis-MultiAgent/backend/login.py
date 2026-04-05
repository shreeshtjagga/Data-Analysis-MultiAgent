import streamlit as st
import logging
from datetime import datetime
from auth import login_user, register_user
from db import init_db

logger = logging.getLogger(__name__)

# Initialize database on first run
if "db_initialized" not in st.session_state:
    init_db()
    st.session_state.db_initialized = True


def init_session_state():
    """Initialize session state variables."""
    defaults = {
        "logged_in": False,
        "user_id": None,
        "user_email": None,
        "login_time": None,
        "auth_mode": "login",  # "login" or "register"
        "show_password": False,
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def login_page():
    """Render the login/registration page."""
    init_session_state()

    # Set page config
    st.set_page_config(
        page_title="AI Data Analyst - Login",
        layout="centered",
        initial_sidebar_state="collapsed",
    )

    # Custom CSS for authentication page
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Syne:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500&family=Outfit:wght@300;400;500;600&display=swap');

    * {
        margin: 0;
        padding: 0;
        box-sizing: border-box;
    }

    html, body, [class*="css"] {
        font-family: 'Outfit', sans-serif;
        background: linear-gradient(135deg, #080c14 0%, #0f1419 50%, #0a0e18 100%);
        color: #e2e8f0;
    }

    .stApp {
        background: linear-gradient(135deg, #080c14 0%, #0f1419 50%, #0a0e18 100%) !important;
    }

    [data-testid="stAppViewContainer"] {
        padding: 0 !important;
        background: transparent !important;
    }

    [data-testid="stAppViewContainer"] .main {
        display: flex;
        align-items: center;
        justify-content: center;
        min-height: 100vh;
        background: transparent !important;
        padding: 20px !important;
    }

    .auth-container {
        width: 100%;
        max-width: 420px;
        background: rgba(15, 23, 42, 0.9);
        backdrop-filter: blur(10px);
        border: 1px solid rgba(99, 102, 241, 0.2);
        border-radius: 16px;
        padding: 48px 32px;
        box-shadow: 0 25px 50px -12px rgba(0, 0, 0, 0.5),
                    0 0 40px rgba(99, 102, 241, 0.05);
    }

    .auth-header {
        text-align: center;
        margin-bottom: 36px;
    }

    .auth-logo {
        font-size: 3rem;
        margin-bottom: 16px;
        background: linear-gradient(135deg, #6366f1 0%, #a5b4fc 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
    }

    .auth-title {
        font-family: 'Syne', sans-serif;
        font-size: 1.8rem;
        font-weight: 800;
        color: #f1f5f9;
        margin-bottom: 8px;
        letter-spacing: -0.02em;
    }

    .auth-subtitle {
        font-size: 0.9rem;
        color: #64748b;
        letter-spacing: 0.05em;
    }

    .auth-form {
        display: flex;
        flex-direction: column;
        gap: 16px;
    }

    .form-group {
        display: flex;
        flex-direction: column;
        gap: 8px;
    }

    .form-label {
        font-size: 0.85rem;
        font-weight: 600;
        color: #cbd5e1;
        text-transform: uppercase;
        letter-spacing: 0.08em;
    }

    .form-input {
        padding: 12px 14px;
        border: 1px solid rgba(99, 102, 241, 0.2);
        border-radius: 8px;
        background: rgba(15, 23, 42, 0.8);
        color: #e2e8f0;
        font-size: 0.95rem;
        transition: all 0.2s;
    }

    .form-input:focus {
        outline: none;
        border-color: #6366f1;
        background: rgba(15, 23, 42, 0.95);
        box-shadow: 0 0 0 3px rgba(99, 102, 241, 0.1);
    }

    .form-input::placeholder {
        color: #475569;
    }

    .btn-primary {
        padding: 12px 20px;
        margin-top: 8px;
        border: none;
        border-radius: 8px;
        background: linear-gradient(135deg, #6366f1 0%, #4f46e5 100%);
        color: white;
        font-family: 'Syne', sans-serif;
        font-weight: 700;
        font-size: 0.9rem;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        cursor: pointer;
        transition: all 0.2s;
        box-shadow: 0 4px 15px rgba(99, 102, 241, 0.3);
    }

    .btn-primary:hover {
        background: linear-gradient(135deg, #818cf8 0%, #6366f1 100%);
        box-shadow: 0 6px 20px rgba(99, 102, 241, 0.45);
        transform: translateY(-2px);
    }

    .btn-secondary {
        padding: 10px 16px;
        border: 1px solid rgba(99, 102, 241, 0.3);
        border-radius: 8px;
        background: transparent;
        color: #a5b4fc;
        font-family: 'Syne', sans-serif;
        font-weight: 600;
        font-size: 0.85rem;
        letter-spacing: 0.06em;
        text-transform: uppercase;
        cursor: pointer;
        transition: all 0.2s;
        margin-top: 4px;
    }

    .btn-secondary:hover {
        background: rgba(99, 102, 241, 0.1);
        border-color: #6366f1;
        color: #e2e8f0;
    }

    .toggle-mode {
        text-align: center;
        margin-top: 20px;
        padding-top: 20px;
        border-top: 1px solid rgba(99, 102, 241, 0.1);
    }

    .toggle-mode-text {
        font-size: 0.85rem;
        color: #64748b;
        margin-bottom: 12px;
    }

    .divider {
        margin: 24px 0;
        position: relative;
        text-align: center;
    }

    .divider::before {
        content: '';
        position: absolute;
        left: 0;
        top: 50%;
        width: 100%;
        height: 1px;
        background: rgba(99, 102, 241, 0.1);
    }

    .divider-text {
        position: relative;
        background: rgba(15, 23, 42, 0.9);
        padding: 0 8px;
        color: #64748b;
        font-size: 0.8rem;
        text-transform: uppercase;
        letter-spacing: 0.08em;
    }

    .error-message {
        padding: 12px 14px;
        border-radius: 8px;
        background: rgba(239, 68, 68, 0.1);
        border: 1px solid rgba(239, 68, 68, 0.3);
        color: #fca5a5;
        font-size: 0.85rem;
        margin-bottom: 16px;
        animation: slideIn 0.3s ease;
    }

    .success-message {
        padding: 12px 14px;
        border-radius: 8px;
        background: rgba(16, 185, 129, 0.1);
        border: 1px solid rgba(16, 185, 129, 0.3);
        color: #6ee7b7;
        font-size: 0.85rem;
        margin-bottom: 16px;
        animation: slideIn 0.3s ease;
    }

    @keyframes slideIn {
        from {
            opacity: 0;
            transform: translateY(-10px);
        }
        to {
            opacity: 1;
            transform: translateY(0);
        }
    }

    .password-strength {
        font-size: 0.75rem;
        color: #64748b;
        margin-top: 4px;
    }

    .strength-weak { color: #ef4444; }
    .strength-fair { color: #f97316; }
    .strength-good { color: #eab308; }
    .strength-strong { color: #10b981; }

    [data-testid="stButton"] { width: 100%; }
    [data-testid="stTextInput"] { width: 100%; }
    </style>
    """, unsafe_allow_html=True)

    # Create centered container
    col1, col2, col3 = st.columns([1, 1.2, 1])

    with col2:
        st.markdown("""
        <div class="auth-container">
        """, unsafe_allow_html=True)

        # Header
        st.markdown("""
        <div class="auth-header">
            <div class="auth-logo">◈</div>
            <div class="auth-title">Data Analyst</div>
            <div class="auth-subtitle">AI-Powered Data Analysis</div>
        </div>
        """, unsafe_allow_html=True)

        # Tabs for login/register
        tab1, tab2 = st.tabs(["LOGIN", "REGISTER"])

        with tab1:
            st.markdown('<div class="auth-form">', unsafe_allow_html=True)
            
            email = st.text_input(
                "Email Address",
                key="login_email",
                placeholder="you@example.com",
                help="Enter your registered email"
            )
            
            password = st.text_input(
                "Password",
                type="password",
                key="login_password",
                placeholder="••••••••",
                help="Enter your password"
            )

            if st.button("Sign In", use_container_width=True, type="primary"):
                if not email or not password:
                    st.markdown(
                        '<div class="error-message">Please enter both email and password</div>',
                        unsafe_allow_html=True
                    )
                else:
                    result = login_user(email, password)
                    if result["success"]:
                        st.session_state.logged_in = True
                        st.session_state.user_id = result["user"]["id"]
                        st.session_state.user_email = result["user"]["email"]
                        st.session_state.login_time = datetime.now()
                        
                        st.markdown(
                            '<div class="success-message">✓ Login successful! Redirecting...</div>',
                            unsafe_allow_html=True
                        )
                        st.balloons()
                        st.rerun()
                    else:
                        st.markdown(
                            f'<div class="error-message">✗ {result["message"]}</div>',
                            unsafe_allow_html=True
                        )

            st.markdown('</div>', unsafe_allow_html=True)

        with tab2:
            st.markdown('<div class="auth-form">', unsafe_allow_html=True)

            reg_email = st.text_input(
                "Email Address",
                key="register_email",
                placeholder="you@example.com",
                help="Choose a unique email"
            )

            reg_password = st.text_input(
                "Password",
                type="password",
                key="register_password",
                placeholder="••••••••",
                help="At least 6 characters"
            )

            reg_password_confirm = st.text_input(
                "Confirm Password",
                type="password",
                key="register_password_confirm",
                placeholder="••••••••",
                help="Re-enter your password"
            )

            # Password strength indicator
            if reg_password:
                strength = assess_password_strength(reg_password)
                strength_class = f"strength-{strength['level']}"
                st.markdown(
                    f'<div class="password-strength {strength_class}">'
                    f'Strength: {strength["label"]} {strength["icon"]}'
                    f'</div>',
                    unsafe_allow_html=True
                )

            if st.button("Create Account", use_container_width=True, type="primary"):
                if not reg_email or not reg_password or not reg_password_confirm:
                    st.markdown(
                        '<div class="error-message">Please fill in all fields</div>',
                        unsafe_allow_html=True
                    )
                elif reg_password != reg_password_confirm:
                    st.markdown(
                        '<div class="error-message">Passwords do not match</div>',
                        unsafe_allow_html=True
                    )
                elif len(reg_password) < 6:
                    st.markdown(
                        '<div class="error-message">Password must be at least 6 characters</div>',
                        unsafe_allow_html=True
                    )
                else:
                    result = register_user(reg_email, reg_password)
                    if result["success"]:
                        st.markdown(
                            '<div class="success-message">✓ Registration successful! Please log in.</div>',
                            unsafe_allow_html=True
                        )
                        st.info("Account created! Use your credentials to log in.")
                    else:
                        st.markdown(
                            f'<div class="error-message">✗ {result["message"]}</div>',
                            unsafe_allow_html=True
                        )

            st.markdown('</div>', unsafe_allow_html=True)

        st.markdown("""
        </div>
        """, unsafe_allow_html=True)


def assess_password_strength(password: str) -> dict:
    """Assess password strength."""
    score = 0
    feedback = []

    if len(password) >= 8:
        score += 1
    if len(password) >= 12:
        score += 1
    if any(c.isupper() for c in password):
        score += 1
    if any(c.isdigit() for c in password):
        score += 1
    if any(c in "!@#$%^&*" for c in password):
        score += 1

    if score <= 2:
        return {"level": "weak", "label": "Weak", "icon": "⚠️"}
    elif score <= 3:
        return {"level": "fair", "label": "Fair", "icon": "◐"}
    elif score <= 4:
        return {"level": "good", "label": "Good", "icon": "◑"}
    else:
        return {"level": "strong", "label": "Strong", "icon": "✓"}


def check_authentication() -> bool:
    """Check if user is logged in."""
    return st.session_state.get("logged_in", False)


def get_current_user():
    """Get current logged-in user info."""
    if check_authentication():
        return {
            "id": st.session_state.get("user_id"),
            "email": st.session_state.get("user_email"),
            "login_time": st.session_state.get("login_time")
        }
    return None


def logout():
    """Logout the current user."""
    st.session_state.logged_in = False
    st.session_state.user_id = None
    st.session_state.user_email = None
    st.session_state.login_time = None
    st.rerun()


# Run login page if not authenticated
if __name__ == "__main__":
    if not check_authentication():
        login_page()
    else:
        st.switch_page("pages/app.py")
