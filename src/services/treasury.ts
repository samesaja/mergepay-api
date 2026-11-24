/**
 * Unified Treasury Service — Issue #884.
 *
 * Coordinates group treasury operations, multisig proposals, signer configuration,
 * and comprehensive audit logging.
 */

import { prisma } from "../db";
import { Prisma } from "@prisma/client";
import {
  auditTreasurySignerChange,
  auditMultisigActionTx,
  logActionTx,
  type TreasurySignerChangeAuditParams,
  type MultisigAuditParams,
  type AuditOutcome,
} from "./audit";
import { AuditAction } from "./audit-actions";
import { sanitizeAuditMetadata } from "../lib/audit-logger";
import { treasurySignaturesService } from "./treasury-signatures";
import { treasuryProposalsService } from "./treasury-proposals";
import { verifyTreasuryProposalSignature } from "./treasury-signature-verification";
import { reconcileTreasuryBalance } from "./treasuryService";
import {
  validateProposedSignerConfig,
  validateSignerChangeAgainstAccount,
  snapshotToSignerConfig,
  type ProposedSignerConfig,
} from "./treasury-validation";

export {
  verifyTreasuryProposalSignature,
  treasurySignaturesService,
  treasuryProposalsService,
  reconcileTreasuryBalance,
  auditTreasurySignerChange,
  auditMultisigActionTx,
  validateProposedSignerConfig,
  validateSignerChangeAgainstAccount,
  snapshotToSignerConfig,
};

export type {
  TreasurySignerChangeAuditParams,
  MultisigAuditParams,
  ProposedSignerConfig,
};

export interface TreasuryProposalStateMutationAuditParams {
  actorId?: string | null;
  actorPublicKey?: string | null;
  groupId: string;
  proposalId: string;
  action:
    | typeof AuditAction.TREASURY_PROPOSAL_CREATED
    | typeof AuditAction.TREASURY_PROPOSAL_SIGNED
    | typeof AuditAction.TREASURY_PROPOSAL_SUBMITTED
    | typeof AuditAction.TREASURY_PROPOSAL_EXPIRED
    | typeof AuditAction.TREASURY_PROPOSAL_FAILED
    | typeof AuditAction.TREASURY_TX_PROPOSAL_CREATED
    | typeof AuditAction.TREASURY_TX_PROPOSAL_SIGNATURE_ADDED
    | typeof AuditAction.TREASURY_TX_PROPOSAL_READY
    | typeof AuditAction.TREASURY_TX_PROPOSAL_SUBMITTED
    | typeof AuditAction.TREASURY_TX_PROPOSAL_FAILED;
  resourceType?: "treasury_proposal" | "treasury_tx_proposal";
  previousStatus?: string | null;
  nextStatus?: string | null;
  outcome?: AuditOutcome;
  correlationId?: string | null;
  metadata?: Record<string, unknown>;
}

/**
 * Transactional wrapper that executes a treasury multisig proposal state
 * mutation alongside an atomic `logActionTx` audit write (#1182).
 *
 * Ensures that if either the mutation or the audit write fails, the entire
 * database transaction rolls back together. Sensitive keys, tokens, and signed
 * XDRs are scrubbed from metadata before persistence.
 */
export async function withTreasuryProposalAuditTx<T>(
  params: TreasuryProposalStateMutationAuditParams,
  mutate: (tx: Prisma.TransactionClient) => Promise<T>,
  txClient?: Prisma.TransactionClient
): Promise<T> {
  const runner = async (tx: Prisma.TransactionClient): Promise<T> => {
    const result = await mutate(tx);
    const sanitizedExtra = sanitizeAuditMetadata(params.metadata ?? {});
    const safeExtra =
      sanitizedExtra && typeof sanitizedExtra === "object" && !Array.isArray(sanitizedExtra)
        ? (sanitizedExtra as Record<string, unknown>)
        : {};

    await logActionTx(tx, {
      actorId: params.actorId ?? null,
      actorPublicKey: params.actorPublicKey ?? null,
      groupId: params.groupId,
      action: params.action,
      resourceType: params.resourceType ?? "treasury_proposal",
      resourceId: params.proposalId,
      correlationId: params.correlationId ?? null,
      outcome: params.outcome ?? "success",
      metadata: {
        ...(params.previousStatus !== undefined
          ? { previousStatus: params.previousStatus }
          : {}),
        ...(params.nextStatus !== undefined ? { nextStatus: params.nextStatus } : {}),
        ...safeExtra,
      },
    });

    return result;
  };

  if (txClient) {
    return runner(txClient);
  }
  return prisma.$transaction(runner);
}

export interface UpdateTreasurySignersParams {
  groupId: string;
  userId: string;
  actorPublicKey?: string | null;
  treasuryAccountPublicKey: string;
  requiredSigners: number;
  signers?: Array<{ publicKey: string; weight: number }>;
}

/**
 * Updates treasury signer configuration with atomic audit logging.
 */
export async function updateTreasurySigners(
  params: UpdateTreasurySignersParams,
  txClient?: Prisma.TransactionClient
): Promise<void> {
  const runner = async (tx: Prisma.TransactionClient) => {
    const existing = await tx.group.findUnique({
      where: { id: params.groupId },
      select: { treasuryRequiredSigners: true, treasuryAccountPublicKey: true },
    });

    await tx.group.update({
      where: { id: params.groupId },
      data: {
        treasuryEnabled: true,
        treasuryAccountPublicKey: params.treasuryAccountPublicKey,
        treasuryRequiredSigners: params.requiredSigners,
      },
    });

    await auditTreasurySignerChange(
      {
        userId: params.userId,
        actorPublicKey: params.actorPublicKey ?? null,
        groupId: params.groupId,
        treasuryAccountPublicKey: params.treasuryAccountPublicKey,
        requiredSigners: params.requiredSigners,
        previousRequiredSigners: existing?.treasuryRequiredSigners ?? null,
        signers: params.signers,
      },
      tx
    );
  };

  if (txClient) {
    await runner(txClient);
  } else {
    await prisma.$transaction(runner);
  }
}
