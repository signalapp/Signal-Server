package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class StickerPackFormUploadAttributes {

  @JsonProperty
  private StickerPackFormUploadItem       manifest;

  @JsonProperty
  private List<StickerPackFormUploadItem> stickers;

  @JsonProperty
  private String                          packId;

  public StickerPackFormUploadAttributes() {}

  public StickerPackFormUploadAttributes(String packId, StickerPackFormUploadItem manifest, List<StickerPackFormUploadItem> stickers) {
    this.packId   = packId;
    this.manifest = manifest;
    this.stickers = stickers;
  }

  public StickerPackFormUploadItem getManifest() {
    return manifest;
  }

  public List<StickerPackFormUploadItem> getStickers() {
    return stickers;
  }

  public String getPackId() {
    return packId;
  }

  public static class StickerPackFormUploadItem {
    @JsonProperty
    private int id;

    @JsonProperty
    private String key;

    @JsonProperty
    private String credential;

    @JsonProperty
    private String acl;

    @JsonProperty
    private String algorithm;

    @JsonProperty
    private String date;

    @JsonProperty
    private String policy;

    @JsonProperty
    private String signature;

    public StickerPackFormUploadItem() {}

    public StickerPackFormUploadItem(int id, String key, String credential, String acl, String algorithm, String date, String policy, String signature) {
      this.key        = key;
      this.credential = credential;
      this.acl        = acl;
      this.algorithm  = algorithm;
      this.date       = date;
      this.policy     = policy;
      this.signature  = signature;
      this.id         = id;
    }

    public String getKey() {
      return key;
    }

    public String getCredential() {
      return credential;
    }

    public String getAcl() {
      return acl;
    }

    public String getAlgorithm() {
      return algorithm;
    }

    public String getDate() {
      return date;
    }

    public String getPolicy() {
      return policy;
    }

    public String getSignature() {
      return signature;
    }

    public int getId() {
      return id;
    }
  }

}
