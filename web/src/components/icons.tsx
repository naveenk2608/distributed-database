// Inline stroke icons (16px grid, currentColor), decorative: each sits next
// to a text label, so they are hidden from assistive technology.
import type { ReactNode } from "react";

function Icon({ children, className = "h-4 w-4" }: { children: ReactNode; className?: string }) {
  return (
    <svg
      viewBox="0 0 16 16"
      fill="none"
      stroke="currentColor"
      strokeWidth={1.5}
      strokeLinecap="round"
      strokeLinejoin="round"
      className={className}
      aria-hidden="true"
      focusable="false"
    >
      {children}
    </svg>
  );
}

type P = { className?: string };

export const OverviewIcon = ({ className }: P) => (
  <Icon className={className}>
    <rect x="2" y="2" width="5" height="5" rx="1" />
    <rect x="9" y="2" width="5" height="5" rx="1" />
    <rect x="2" y="9" width="5" height="5" rx="1" />
    <rect x="9" y="9" width="5" height="5" rx="1" />
  </Icon>
);

export const ShardsIcon = ({ className }: P) => (
  <Icon className={className}>
    <path d="M8 2 14 5 8 8 2 5Z" />
    <path d="m2 8 6 3 6-3" />
    <path d="m2 11 6 3 6-3" />
  </Icon>
);

export const NodesIcon = ({ className }: P) => (
  <Icon className={className}>
    <rect x="2" y="2.5" width="12" height="4.5" rx="1" />
    <rect x="2" y="9" width="12" height="4.5" rx="1" />
    <path d="M4.5 4.75h.01M4.5 11.25h.01" strokeWidth={2} />
  </Icon>
);

export const EventsIcon = ({ className }: P) => (
  <Icon className={className}>
    <path d="M1.5 8h3l2-5 3 10 2-5h3" />
  </Icon>
);

export const SunIcon = ({ className }: P) => (
  <Icon className={className}>
    <circle cx="8" cy="8" r="3" />
    <path d="M8 1.5v1.5M8 13v1.5M1.5 8H3M13 8h1.5M3.4 3.4l1 1M11.6 11.6l1 1M3.4 12.6l1-1M11.6 4.4l1-1" />
  </Icon>
);

export const MoonIcon = ({ className }: P) => (
  <Icon className={className}>
    <path d="M13.5 9.5A5.5 5.5 0 0 1 6.5 2.5a5.5 5.5 0 1 0 7 7Z" />
  </Icon>
);

export const CopyIcon = ({ className }: P) => (
  <Icon className={className}>
    <rect x="5.5" y="5.5" width="8" height="8" rx="1.5" />
    <path d="M10.5 5.5v-2a1 1 0 0 0-1-1h-6a1 1 0 0 0-1 1v6a1 1 0 0 0 1 1h2" />
  </Icon>
);

export const CheckIcon = ({ className }: P) => (
  <Icon className={className}>
    <path d="m3 8.5 3 3 7-7" />
  </Icon>
);

export const AlertIcon = ({ className }: P) => (
  <Icon className={className}>
    <path d="M8 1.75 14.75 13.5H1.25Z" />
    <path d="M8 6.25v3M8 11.5h.01" />
  </Icon>
);

export const SearchIcon = ({ className }: P) => (
  <Icon className={className}>
    <circle cx="7" cy="7" r="4.5" />
    <path d="m13.5 13.5-3.3-3.3" />
  </Icon>
);

export const ChevronIcon = ({ className }: P) => (
  <Icon className={className}>
    <path d="m6 4 4 4-4 4" />
  </Icon>
);

export const InboxIcon = ({ className }: P) => (
  <Icon className={className}>
    <path d="M2 9.5 4 3h8l2 6.5V13H2Z" />
    <path d="M2 9.5h3.5l1 1.5h3l1-1.5H14" />
  </Icon>
);

// The sidebar's mark: a stack of disks on the accent color.
export const LogoMark = ({ className = "h-6 w-6" }: P) => (
  <svg viewBox="0 0 24 24" className={className} aria-hidden="true" focusable="false">
    <rect width="24" height="24" rx="6" fill="rgb(var(--accent))" />
    <g fill="none" stroke="#fff" strokeWidth="1.6" strokeLinecap="round">
      <ellipse cx="12" cy="7.5" rx="5" ry="2" />
      <path d="M7 7.5v4.5c0 1.1 2.24 2 5 2s5-.9 5-2V7.5" />
      <path d="M7 12v4.5c0 1.1 2.24 2 5 2s5-.9 5-2V12" />
    </g>
  </svg>
);
