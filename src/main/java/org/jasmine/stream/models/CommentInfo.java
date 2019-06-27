package org.jasmine.stream.models;

import org.jasmine.stream.utils.JSONStringable;

/**
 * CommentInfo class
 * It represents information provided by dataset
 */
public class CommentInfo implements JSONStringable {

    private long approveDate;
    private String articleID;
    private long articleWordCount;
    private long commentID;
    private CommentType commentType;
    private long createDate;
    private int depth;
    private boolean editorsSelection;
    private long inReplyTo;
    private String parentUserDisplayName;
    private long recommendations;
    private String sectionName;
    private String userDisplayName;
    private long userID;
    private String userLocation;

    public CommentInfo() {
    }

    public long getApproveDate() {
        return approveDate;
    }

    public void setApproveDate(long approveDate) {
        this.approveDate = approveDate;
    }

    public String getArticleID() {
        return articleID;
    }

    public void setArticleID(String articleID) {
        this.articleID = articleID;
    }

    public long getArticleWordCount() {
        return articleWordCount;
    }

    public void setArticleWordCount(long articleWordCount) {
        this.articleWordCount = articleWordCount;
    }

    public long getCommentID() {
        return commentID;
    }

    public void setCommentID(long commentID) {
        this.commentID = commentID;
    }

    public CommentType getCommentType() {
        return commentType;
    }

    public void setCommentType(CommentType commentType) {
        this.commentType = commentType;
    }

    public long getCreateDate() {
        return createDate;
    }

    public void setCreateDate(long createDate) {
        this.createDate = createDate;
    }

    public int getDepth() {
        return depth;
    }

    public void setDepth(int depth) {
        this.depth = depth;
    }

    public boolean isEditorsSelection() {
        return editorsSelection;
    }

    public void setEditorsSelection(boolean editorsSelection) {
        this.editorsSelection = editorsSelection;
    }

    public long getInReplyTo() {
        return inReplyTo;
    }

    public void setInReplyTo(long inReplyTo) {
        this.inReplyTo = inReplyTo;
    }

    public String getParentUserDisplayName() {
        return parentUserDisplayName;
    }

    public void setParentUserDisplayName(String parentUserDisplayName) {
        this.parentUserDisplayName = parentUserDisplayName;
    }

    public long getRecommendations() {
        return recommendations;
    }

    public void setRecommendations(long recommendations) {
        this.recommendations = recommendations;
    }

    public String getSectionName() {
        return sectionName;
    }

    public void setSectionName(String sectionName) {
        this.sectionName = sectionName;
    }

    public String getUserDisplayName() {
        return userDisplayName;
    }

    public void setUserDisplayName(String userDisplayName) {
        this.userDisplayName = userDisplayName;
    }

    public long getUserID() {
        return userID;
    }

    public void setUserID(long userID) {
        this.userID = userID;
    }

    public String getUserLocation() {
        return userLocation;
    }

    public void setUserLocation(String userLocation) {
        this.userLocation = userLocation;
    }

    @Override
    public String toString() {
        return this.toJSONString();
    }
}
