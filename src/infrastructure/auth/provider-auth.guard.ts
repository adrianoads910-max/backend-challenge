import { type CanActivate, type ExecutionContext, Inject, Injectable, SetMetadata } from "@nestjs/common";
import { Reflector } from "@nestjs/core";

/** Identity of the caller as established by the IdP (e.g. Keycloak client-credentials token). */
export interface ProviderIdentity {
  /** OAuth client id / subject. */
  subject: string;
  /** Provider ids this caller may act for (token claim, e.g. `provider_ids`). */
  providerIds: string[];
}

/**
 * Extension point for authentication (section 2 — intentionally not implemented, see
 * ARCHITECTURE.md "Authentication"). A real implementation validates a bearer JWT against the
 * IdP's JWKS (issuer, audience, expiry) and returns the provider identity, or undefined.
 */
export interface ProviderIdentityPort {
  authenticate(authorizationHeader: string | undefined): Promise<ProviderIdentity | undefined>;
}

export const PROVIDER_IDENTITY = Symbol("ProviderIdentityPort");
export const PUBLIC_ROUTE = "publicRoute";
export const Public = () => SetMetadata(PUBLIC_ROUTE, true);

/** No-op: every caller is trusted. Swap for a JWKS-backed implementation in production. */
export class TrustAllProviderIdentity implements ProviderIdentityPort {
  async authenticate(): Promise<ProviderIdentity> {
    return { subject: "anonymous", providerIds: ["*"] };
  }
}

@Injectable()
export class ProviderAuthGuard implements CanActivate {
  constructor(
    private readonly reflector: Reflector,
    @Inject(PROVIDER_IDENTITY) private readonly identity: ProviderIdentityPort,
  ) {}

  async canActivate(context: ExecutionContext): Promise<boolean> {
    const isPublic = this.reflector.getAllAndOverride<boolean>(PUBLIC_ROUTE, [context.getHandler(), context.getClass()]);
    if (isPublic) return true;
    const req = context.switchToHttp().getRequest<{ headers: Record<string, string | undefined>; provider?: ProviderIdentity }>();
    const identity = await this.identity.authenticate(req.headers.authorization);
    if (!identity) return false;
    // A real implementation would also check body.providerId ∈ identity.providerIds (→ 403).
    req.provider = identity;
    return true;
  }
}
