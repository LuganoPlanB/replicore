import assert from "node:assert/strict"
import test from "node:test"

import {
  normalizeInterfaceFamily,
  normalizeNetworkInterfaces
} from "../src/setup/network-interfaces.js"

test("normalizeInterfaceFamily accepts string and numeric families", () => {
  assert.equal(normalizeInterfaceFamily("IPv4"), "IPv4")
  assert.equal(normalizeInterfaceFamily("IPv6"), "IPv6")
  assert.equal(normalizeInterfaceFamily(4), "IPv4")
  assert.equal(normalizeInterfaceFamily(6), "IPv6")
  assert.equal(normalizeInterfaceFamily("Other"), null)
})

test("normalizeNetworkInterfaces returns sorted setup records", () => {
  const records = normalizeNetworkInterfaces({
    lo: [
      {
        address: "127.0.0.1",
        netmask: "255.0.0.0",
        family: "IPv4",
        mac: "00:00:00:00:00:00",
        internal: true,
        cidr: "127.0.0.1/8"
      }
    ],
    eth0: [
      {
        address: "fe80::1",
        netmask: "ffff:ffff:ffff:ffff::",
        family: 6,
        mac: "aa:bb:cc:dd:ee:ff",
        internal: false,
        cidr: "fe80::1/64"
      },
      {
        address: "192.168.1.10",
        netmask: "255.255.255.0",
        family: 4,
        mac: "aa:bb:cc:dd:ee:ff",
        internal: false,
        cidr: "192.168.1.10/24"
      }
    ],
    wlan0: [
      {
        address: "10.0.0.7",
        netmask: "255.255.255.0",
        family: "IPv4",
        mac: "11:22:33:44:55:66",
        internal: false,
        cidr: "10.0.0.7/24"
      },
      {
        address: "n/a",
        netmask: "",
        family: "Other",
        mac: "",
        internal: false,
        cidr: null
      }
    ]
  })

  assert.deepEqual(records, [
    {
      name: "eth0",
      family: "IPv4",
      address: "192.168.1.10",
      netmask: "255.255.255.0",
      mac: "aa:bb:cc:dd:ee:ff",
      internal: false,
      cidr: "192.168.1.10/24",
      eligibleForBind: true
    },
    {
      name: "wlan0",
      family: "IPv4",
      address: "10.0.0.7",
      netmask: "255.255.255.0",
      mac: "11:22:33:44:55:66",
      internal: false,
      cidr: "10.0.0.7/24",
      eligibleForBind: true
    },
    {
      name: "eth0",
      family: "IPv6",
      address: "fe80::1",
      netmask: "ffff:ffff:ffff:ffff::",
      mac: "aa:bb:cc:dd:ee:ff",
      internal: false,
      cidr: "fe80::1/64",
      eligibleForBind: true
    },
    {
      name: "lo",
      family: "IPv4",
      address: "127.0.0.1",
      netmask: "255.0.0.0",
      mac: "00:00:00:00:00:00",
      internal: true,
      cidr: "127.0.0.1/8",
      eligibleForBind: false
    }
  ])
})
