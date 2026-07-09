import { useEffect } from "react";
import { useNavigate } from "react-router-dom";

// OAuth callback page — no-op in local JWT auth mode, just redirect to login
export default function AuthCallback() {
  const navigate = useNavigate();

  useEffect(() => {
    navigate("/login", { replace: true });
  }, [navigate]);

  return null;
}
