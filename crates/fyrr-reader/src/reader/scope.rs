//! A transport binding and its completed admission are distinct states.
use fyrr_core::publication::GenerationPointer;

#[derive(Debug, Clone)]
enum Subject {
    Stdio {
        admitted: Option<GenerationPointer>,
    },
    Unbound,
    Binding {
        key: String,
        expected: Option<GenerationPointer>,
    },
    Http {
        key: String,
        principal: String,
        admitted: Option<GenerationPointer>,
    },
}

#[derive(Debug, Clone)]
pub(super) struct RequestScope {
    subject: Subject,
}
impl RequestScope {
    pub(super) fn new(http: bool) -> Self {
        Self {
            subject: if http {
                Subject::Unbound
            } else {
                Subject::Stdio { admitted: None }
            },
        }
    }
    pub(super) fn principal(&self) -> Option<&str> {
        match &self.subject {
            Subject::Http { principal, .. } => Some(principal),
            Subject::Stdio { .. } | Subject::Unbound | Subject::Binding { .. } => None,
        }
    }
    pub(super) fn http_key(&self) -> Option<&String> {
        match &self.subject {
            Subject::Binding { key, .. } | Subject::Http { key, .. } => Some(key),
            Subject::Stdio { .. } | Subject::Unbound => None,
        }
    }
    /// Preflight compares the caller's expectation during binding. A resolved
    /// subject compares only the identity of its completed admission.
    pub(super) fn identity_for_refresh(&self) -> Option<&GenerationPointer> {
        match &self.subject {
            Subject::Binding { expected, .. } => expected.as_ref(),
            Subject::Stdio { admitted } | Subject::Http { admitted, .. } => admitted.as_ref(),
            Subject::Unbound => None,
        }
    }
    pub(super) fn begin(&mut self, subject: &crate::auth::AuthenticatedSubject) {
        self.refuse();
        self.subject = Subject::Binding {
            key: subject.key.clone(),
            expected: subject.identity.clone(),
        };
    }
    pub(super) fn resolve(&mut self, principal: String) {
        let Subject::Binding { key, .. } = std::mem::replace(&mut self.subject, Subject::Unbound)
        else {
            unreachable!("subject binding precedes principal resolution")
        };
        self.subject = Subject::Http {
            key,
            principal,
            admitted: None,
        };
    }
    pub(super) fn invalidate_principal(&mut self) {
        self.refuse();
        self.subject = match std::mem::replace(&mut self.subject, Subject::Unbound) {
            Subject::Http { key, .. } => Subject::Binding {
                key,
                expected: None,
            },
            other => other,
        };
    }
    pub(super) fn admitted(&mut self, identity: GenerationPointer) {
        match &mut self.subject {
            Subject::Stdio { admitted } | Subject::Http { admitted, .. } => {
                *admitted = Some(identity);
            }
            Subject::Unbound | Subject::Binding { .. } => {
                unreachable!("a resolved subject precedes completed admission")
            }
        }
    }
    pub(super) fn refuse(&mut self) {
        match &mut self.subject {
            Subject::Stdio { admitted } | Subject::Http { admitted, .. } => *admitted = None,
            Subject::Binding { expected, .. } => *expected = None,
            Subject::Unbound => {}
        }
    }
    pub(super) fn without_admission(&self) -> Self {
        Self {
            subject: match &self.subject {
                Subject::Stdio { .. } => Subject::Stdio { admitted: None },
                Subject::Unbound => Subject::Unbound,
                Subject::Binding { key, .. } => Subject::Binding {
                    key: key.clone(),
                    expected: None,
                },
                Subject::Http { key, principal, .. } => Subject::Http {
                    key: key.clone(),
                    principal: principal.clone(),
                    admitted: None,
                },
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity() -> GenerationPointer {
        GenerationPointer {
            generation: 7,
            object: "identity.json".into(),
            digest: "digest".into(),
        }
    }

    #[test]
    fn binding_expectation_is_not_completed_admission() {
        let identity = identity();
        let subject = crate::auth::AuthenticatedSubject {
            key: "issuer:subject".into(),
            expires: i64::MAX,
            identity: Some(identity.clone()),
            api_key: None,
        };
        let mut scope = RequestScope::new(true);
        assert!(scope.http_key().is_none());
        scope.begin(&subject);
        assert!(matches!(scope.subject, Subject::Binding { .. }));
        assert_eq!(scope.identity_for_refresh(), Some(&identity));
        assert!(scope.principal().is_none());
        scope.resolve("principal".into());
        assert_eq!(scope.principal(), Some("principal"));
        assert!(scope.identity_for_refresh().is_none());
        scope.admitted(identity.clone());
        assert_eq!(scope.identity_for_refresh(), Some(&identity));
        scope.begin(&subject);
        assert!(matches!(scope.subject, Subject::Binding { .. }));
        assert!(scope.principal().is_none());
        scope.refuse();
        assert!(scope.identity_for_refresh().is_none());
    }

    #[test]
    fn invalidation_and_worker_cloning_never_carry_admission() {
        let identity = identity();
        let mut stdio = RequestScope::new(false);
        stdio.admitted(identity.clone());
        assert!(stdio.without_admission().identity_for_refresh().is_none());
        assert_eq!(stdio.identity_for_refresh(), Some(&identity));
        let mut http = RequestScope::new(true);
        http.begin(&crate::auth::AuthenticatedSubject {
            key: "issuer:subject".into(),
            expires: i64::MAX,
            identity: None,
            api_key: None,
        });
        assert!(http.identity_for_refresh().is_none());
        http.resolve("principal".into());
        http.admitted(identity);
        let worker = http.without_admission();
        assert_eq!(worker.principal(), Some("principal"));
        assert_eq!(worker.http_key(), http.http_key());
        assert!(worker.identity_for_refresh().is_none());
        http.invalidate_principal();
        assert!(http.principal().is_none());
        assert!(http.identity_for_refresh().is_none());
        assert_eq!(http.http_key().map(String::as_str), Some("issuer:subject"));
    }
}
