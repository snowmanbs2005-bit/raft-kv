package kvstore

import "net/http"

// redirectToLeader responds with an HTTP 307 redirect to the current
// leader's address, preserving method and body (307, unlike 301/302,
// mandates that clients resend the original method+body -- important here
// since PUT/DELETE carry state that must not be dropped or turned into a
// GET). If no leader is currently known, it reports 503 so the client can
// retry shortly.
func (s *Server) redirectToLeader(w http.ResponseWriter, r *http.Request) {
	leaderID := s.node.LeaderHint()
	if leaderID == "" {
		http.Error(w, "no leader currently known, retry shortly", http.StatusServiceUnavailable)
		return
	}
	addr, ok := s.leaderAddrOf(leaderID)
	if !ok {
		http.Error(w, "leader address unknown, retry shortly", http.StatusServiceUnavailable)
		return
	}
	url := "http://" + addr + r.URL.RequestURI()
	http.Redirect(w, r, url, http.StatusTemporaryRedirect)
}
